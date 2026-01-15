#include <string>

#include "http_to_kafka_filter.h"

#include "envoy/server/filter_config.h"

#include <cstdint>

extern "C" uint64_t set_config(const uint8_t* bootstraps, uint64_t bootstraps_len);
extern "C" uint64_t produce(const uint8_t* topic_name, uint64_t topic_name_len,
                            const uint8_t* payload, uint64_t payload_len,
                            uint8_t** err_msg_ptr, uint64_t& err_msg_len);
extern "C" uint64_t consume(const uint8_t* topic_name, uint64_t topic_name_len,
                            uint8_t** out_ptr, uint64_t& out_len,
                            uint8_t** err_msg_ptr, uint64_t& err_msg_len);
extern "C" void free_buffer(uint8_t* in, uint64_t in_len);

namespace Envoy {
namespace Http {

HttpToKafkaDecoderFilterConfig::HttpToKafkaDecoderFilterConfig(const kafkafilter::HttpToKafka& proto_config)
    : kafka_host_(proto_config.kafka_host()),
      kafka_port_(proto_config.kafka_port()),
      action_header_(proto_config.action_header().empty() ? "x-kafka-action"
                                                          : proto_config.action_header()),
      topic_header_(proto_config.topic_header().empty() ? "x-kafka-topic"
                                                        : proto_config.topic_header()),
      max_payload_bytes_(proto_config.max_payload_bytes()) {
        (void)set_config(reinterpret_cast<const uint8_t*>(proto_config.kafka_host().c_str()),
                                                          proto_config.kafka_host().size());
      }

HttpToKafkaDecoderFilter::HttpToKafkaDecoderFilter(HttpToKafkaDecoderFilterConfigSharedPtr config)
    : config_(config) {}

HttpToKafkaDecoderFilter::~HttpToKafkaDecoderFilter() {}

void HttpToKafkaDecoderFilter::onDestroy() {}

void HttpToKafkaDecoderFilter::sendError(Code code, absl::string_view msg, absl::string_view details) {
  decoder_callbacks_->sendLocalReply(code, msg,
                                    [](ResponseHeaderMap& headers) {
                                      headers.setReferenceContentType("text/plain; charset=utf-8");
                                    },
                                    absl::nullopt, details);
}

FilterHeadersStatus HttpToKafkaDecoderFilter::decodeHeaders(RequestHeaderMap& headers, bool end_stream) {
  const LowerCaseString action_lc(config_->actionHeader());
  const auto action_vals = headers.get(action_lc);
  if (action_vals.empty()) {
    return FilterHeadersStatus::Continue;
  }

  std::string action = std::string(action_vals[0]->value().getStringView());
  absl::AsciiStrToLower(&action);

  if (action == "produce") {
    mode_ = Mode::Produce;
  } else if (action == "consume") {
    mode_ = Mode::Consume;
  } else {
    mode_ = Mode::None;
    return FilterHeadersStatus::Continue;
  }

  const LowerCaseString topic_lc(config_->topicHeader());
  const auto topic_vals = headers.get(topic_lc);
  if (topic_vals.empty()) {
    sendError(Code::BadRequest, "missing topic header", "http_to_kafka_missing_topic");
    return FilterHeadersStatus::StopIteration;
  }
  topic_ = std::string(topic_vals[0]->value().getStringView());

  if (mode_ == Mode::Consume) {
    uint8_t* out_ptr = nullptr;
    uint64_t out_len = 0;
    uint8_t* err_ptr = nullptr;
    uint64_t err_len = 0;

    const uint64_t code = consume(reinterpret_cast<const uint8_t*>(topic_.data()), topic_.size(),
                                  &out_ptr, out_len, &err_ptr, err_len);
    
    std::string body;
    if (code == 200 && out_ptr != nullptr && out_len > 0) {
      body.assign(reinterpret_cast<const char*>(out_ptr), out_len);
    }
    std::string err;
    if (err_ptr != nullptr && err_len > 0) {
      err.assign(reinterpret_cast<const char*>(err_ptr), err_len);
    }

    if (out_ptr) free_buffer(out_ptr, out_len);
    if (err_ptr) free_buffer(err_ptr, err_len);

    if (code != 200) {
      sendError(Code::BadRequest, err.empty() ? "consume failed" : err, "http_to_kafka_consume_failed");
      return FilterHeadersStatus::StopIteration;
    }

    replied_ = true;
    decoder_callbacks_->sendLocalReply(Code::OK, body,
                                      [](ResponseHeaderMap& headers) {
                                        headers.setReferenceContentType("application/json; charset=utf-8");
                                      },
                                      absl::nullopt, "http_to_kafka_consume_ok");
    return FilterHeadersStatus::StopIteration;                              
  }

  if (end_stream) {
    sendError(Code::BadRequest, "empty body", "http_to_kafka_empty_body");
    return FilterHeadersStatus::StopIteration;
  }

  return FilterHeadersStatus::StopIteration;
}

FilterDataStatus HttpToKafkaDecoderFilter::decodeData(Buffer::Instance& data, bool end_stream) {
  if (mode_ != Mode::Produce) {
    return FilterDataStatus::Continue;
  }

  if (config_->maxPayloadBytes() > 0 &&
      (body_.length() + data.length()) > config_->maxPayloadBytes()) {
    data.drain(data.length());
    sendError(Code::PayloadTooLarge, "payload too large", "http_to_kafka_payload_too_large");
    return FilterDataStatus::StopIterationNoBuffer;
  }

  if (data.length() > 0) {
    const void* lin = data.linearize(data.length());
    body_.add(lin, data.length());
    data.drain(data.length());
  }

  if (!end_stream) {
    return FilterDataStatus::StopIterationNoBuffer;
  }

  const void* payload_lin = body_.linearize(body_.length());
  const auto* payload = static_cast<const uint8_t*>(payload_lin);
  const uint64_t payload_len = body_.length();

  uint8_t* err_ptr = nullptr;
  uint64_t err_len = 0;

  const uint64_t code =
      produce(reinterpret_cast<const uint8_t*>(topic_.data()), topic_.size(),
              payload, payload_len, &err_ptr, err_len);

  std::string err;
  if (err_ptr != nullptr && err_len > 0) {
    err.assign(reinterpret_cast<const char*>(err_ptr), err_len);
  }
  if (err_ptr) free_buffer(err_ptr, err_len);

  if (code != 200) {
    sendError(Code::BadRequest, err.empty() ? "produce failed" : err, "http_to_kafka_produce_failed");
    return FilterDataStatus::StopIterationNoBuffer;
  }

  replied_ = true;
  decoder_callbacks_->sendLocalReply(Code::OK, "ok",
                                    [](ResponseHeaderMap& headers) {
                                      headers.setReferenceContentType("text/plain; charset=utf-8");
                                    },
                                    absl::nullopt, "http_to_kafka_produce_ok");

  return FilterDataStatus::StopIterationNoBuffer;
}

void HttpToKafkaDecoderFilter::setDecoderFilterCallbacks(StreamDecoderFilterCallbacks& callbacks) {
  decoder_callbacks_ = &callbacks;
}

} // namespace Http
} // namespace Envoy
