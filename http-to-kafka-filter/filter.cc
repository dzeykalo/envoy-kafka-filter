#include <string>

#include "filter.h"
#include "envoy/server/filter_config.h"
#include "source/common/common/logger.h"
#include "source/common/http/header_map_impl.h"

#include <cstdint>

extern "C" {

  uint64_t set_bootstrap_servers(const uint8_t* ptr, uint64_t len);
  
  uint64_t create_consumer(const uint8_t* topic_ptr, uint64_t topic_len);
  void     destroy_consumer(uint64_t client_id);
  
  const uint8_t* consumer_group_id_ptr(uint64_t client_id);
  uint64_t       consumer_group_id_len(uint64_t client_id);
  
  uint64_t poll_message(uint64_t client_id, uint64_t timeout_ms);
  
  const uint8_t* last_message_ptr(uint64_t client_id);
  uint64_t       last_message_len(uint64_t client_id);
  void           clear_last_message(uint64_t client_id);
  
  const uint8_t* last_error_ptr();
  uint64_t       last_error_len();
  void           clear_last_error();
  
  uint64_t ensure_producer();
  uint64_t produce_message(const uint8_t* topic_ptr, uint64_t topic_len,
                           const uint8_t* payload_ptr, uint64_t payload_len);
  uint64_t producer_poll(uint64_t timeout_ms);
  
}

static std::string get_last_error() {
  const uint8_t* p = last_error_ptr();
  const uint64_t len = last_error_len();
  std::string s;
  if (p && len > 0) {
    s.assign(reinterpret_cast<const char*>(p), static_cast<size_t>(len));
  }
  clear_last_error();
  ENVOY_LOG_MISC(trace, "http_to_kafka last_error: {}", s);
  return s;
}

static std::string poll_kafka_message(uint64_t consumer_id, uint64_t timeout_ms) {
  const uint64_t code = poll_message(consumer_id, timeout_ms);
  if (code != 200) {
    return {};
  }
  const uint8_t* ptr = last_message_ptr(consumer_id);
  const uint64_t len = last_message_len(consumer_id);
  std::string msg;
  if (ptr && len > 0) {
    msg.assign(reinterpret_cast<const char*>(ptr), static_cast<size_t>(len));
  }
  ENVOY_LOG_MISC(trace, "http_to_kafka poll_message: consumer_id={} len={}", consumer_id, len);
  clear_last_message(consumer_id);
  return msg;
}

namespace Envoy {
namespace Http {

HttpToKafkaDecoderFilterConfig::HttpToKafkaDecoderFilterConfig(const kafkafilter::HttpToKafka& proto_config)
    : bootstrap_servers_(proto_config.bootstrap_servers()),
      action_header_(proto_config.action_header().empty() ? "x-kafka-action"
                                                          : proto_config.action_header()),
      topic_header_(proto_config.topic_header().empty() ? "x-kafka-topic"
                                                        : proto_config.topic_header()),
      max_payload_bytes_(proto_config.max_payload_bytes()) {
        ENVOY_LOG_MISC(info, "http_to_kafka config: bootstrap_servers={} action_header={} topic_header={} max_payload_bytes={}",
                       bootstrap_servers_, action_header_, topic_header_, max_payload_bytes_);
        (void)set_bootstrap_servers(
            reinterpret_cast<const uint8_t*>(proto_config.bootstrap_servers().c_str()),
            proto_config.bootstrap_servers().size());
      }

HttpToKafkaDecoderFilter::HttpToKafkaDecoderFilter(HttpToKafkaDecoderFilterConfigSharedPtr config)
    : config_(config) {}

void HttpToKafkaDecoderFilter::onDestroy() {
  ENVOY_LOG_MISC(trace, "http_to_kafka onDestroy: client_id={} consuming={}", client_id_, consuming_.load());
  consuming_ = false;
  if (client_id_) {
    KafkaStreamManager::instance().unsubscribe(client_id_);
  }
}

void HttpToKafkaDecoderFilter::sendError(Code code, absl::string_view msg, absl::string_view details) {
  ENVOY_LOG_MISC(warn, "http_to_kafka sendError: code={} msg={} details={}",
                 static_cast<uint32_t>(code), msg, details);
  decoder_callbacks_->sendLocalReply(code, msg,
                                    [](ResponseHeaderMap& headers) {
                                      headers.setReferenceContentType("text/plain; charset=utf-8");
                                    },
                                    absl::nullopt, details);
}

FilterHeadersStatus HttpToKafkaDecoderFilter::decodeHeaders(RequestHeaderMap& headers, bool end_stream) {
  ENVOY_LOG_MISC(trace, "http_to_kafka decodeHeaders: end_stream={} has_action_header={}",
    end_stream, !headers.get(LowerCaseString(config_->actionHeader())).empty());
  const LowerCaseString action_lc(config_->actionHeader());
  const auto action_vals = headers.get(action_lc);
  if (action_vals.empty()) {
    ENVOY_LOG_MISC(trace, "http_to_kafka decodeHeaders: no action header, continue");
    return FilterHeadersStatus::Continue;
  }

  std::string action = std::string(action_vals[0]->value().getStringView());
  absl::AsciiStrToLower(&action);
  ENVOY_LOG_MISC(trace, "http_to_kafka decodeHeaders: action={}", action);

  if (action == "produce" || action == "producer") {
    mode_ = Mode::Produce;
  } else if (action == "consume" || action == "consumer") {
    mode_ = Mode::Consume;
  } else {
    mode_ = Mode::None;
    ENVOY_LOG_MISC(trace, "http_to_kafka decodeHeaders: unknown action={}, continue", action);
    return FilterHeadersStatus::Continue;
  }

  const LowerCaseString topic_lc(config_->topicHeader());
  const auto topic_vals = headers.get(topic_lc);
  if (topic_vals.empty()) {
    ENVOY_LOG_MISC(warn, "http_to_kafka decodeHeaders: missing topic header");
    sendError(Code::BadRequest, "missing topic header", "http_to_kafka_missing_topic");
    return FilterHeadersStatus::StopIteration;
  }
  topic_ = std::string(topic_vals[0]->value().getStringView());
  ENVOY_LOG_MISC(trace, "http_to_kafka decodeHeaders: topic={}", topic_);

  if (mode_ == Mode::Consume) {
    client_id_ = KafkaStreamManager::instance().subscribe(topic_, [this](const std::string& msg) {
      if (!dispatcher_) {
        ENVOY_LOG_MISC(trace, "http_to_kafka consume: no dispatcher, drop message");
        return;
      }
      dispatcher_->post([this, msg]() {
        if (!consuming_) {
          ENVOY_LOG_MISC(trace, "http_to_kafka consume: not consuming, drop message");
          return;
        }

        Buffer::OwnedImpl out;
        out.add(msg);
        out.add("\n", 1); // NDJSON
        ENVOY_LOG_MISC(trace, "http_to_kafka consume: sending message len={}", msg.size());
        decoder_callbacks_->encodeData(out, false);
      });
    });

    if (client_id_ == 0) {
      const std::string err = get_last_error();
      ENVOY_LOG_MISC(warn, "http_to_kafka consume: subscribe failed topic={} err={}",
        topic_, err);
      sendError(Code::BadRequest, err.empty() ? "create_consumer failed" : err,
                "http_to_kafka_consume_failed");
      return FilterHeadersStatus::StopIteration;
    }
    
    ENVOY_LOG_MISC(trace, "http_to_kafka consume: subscribed topic={} client_id={}",
      topic_, client_id_);
    auto headers = ResponseHeaderMapImpl::create();
    headers->setStatus(200);
    headers->setReferenceContentType("application/x-ndjson");
    headers->addCopy(LowerCaseString("cache-control"), "no-cache");
    decoder_callbacks_->encodeHeaders(std::move(headers), false, "kafka_consume_stream");

    consuming_ = true;
    return FilterHeadersStatus::StopIteration;
  }

  if (end_stream) {
    sendError(Code::BadRequest, "empty body", "http_to_kafka_empty_body");
    return FilterHeadersStatus::StopIteration;
  }

  return FilterHeadersStatus::StopIteration;
}

FilterDataStatus HttpToKafkaDecoderFilter::decodeData(Buffer::Instance& data, bool end_stream) {
  if (mode_ == Mode::Consume) {
    if (data.length() > 0) {
      ENVOY_LOG_MISC(trace, "http_to_kafka consume: draining request body len={}", data.length());
      data.drain(data.length());
    }
    return FilterDataStatus::StopIterationNoBuffer;
  }
  if (mode_ != Mode::Produce) {
    return FilterDataStatus::Continue;
  }

  if (config_->maxPayloadBytes() > 0 &&
      (body_.length() + data.length()) > config_->maxPayloadBytes()) {
    data.drain(data.length());
    ENVOY_LOG_MISC(warn, "http_to_kafka produce: payload too large size={} limit={}",
                   body_.length() + data.length(), config_->maxPayloadBytes());
    sendError(Code::PayloadTooLarge, "payload too large", "http_to_kafka_payload_too_large");
    return FilterDataStatus::StopIterationNoBuffer;
  }

  if (data.length() > 0) {
    const void* lin = data.linearize(data.length());
    body_.add(lin, data.length());
    data.drain(data.length());
  }

  if (!end_stream) {
    ENVOY_LOG_MISC(trace, "http_to_kafka produce: buffering body len={}", body_.length());
    return FilterDataStatus::StopIterationNoBuffer;
  }

  const void* payload_lin = body_.linearize(body_.length());
  const auto* payload = static_cast<const uint8_t*>(payload_lin);
  const uint64_t payload_len = body_.length();

  if (ensure_producer() != 200) {
    const std::string err = get_last_error();
    ENVOY_LOG_MISC(warn, "http_to_kafka produce: ensure_producer failed err={}", err);
    sendError(Code::BadRequest, err.empty() ? "ensure_producer failed" : err,
              "http_to_kafka_produce_failed");
    return FilterDataStatus::StopIterationNoBuffer;
  }

  if (produce_message(reinterpret_cast<const uint8_t*>(topic_.data()), topic_.size(),
                      payload, payload_len) != 200) {
    const std::string err = get_last_error();
    ENVOY_LOG_MISC(warn, "http_to_kafka produce: produce_message failed topic={} err={}", topic_, err);
    sendError(Code::BadRequest, err.empty() ? "produce failed" : err,
              "http_to_kafka_produce_failed");
    return FilterDataStatus::StopIterationNoBuffer;
  }

  ENVOY_LOG_MISC(trace, "http_to_kafka produce: message sent topic={} len={}", topic_, payload_len);
  producer_poll(200);

  replied_ = true;
  decoder_callbacks_->sendLocalReply(Code::OK, std::string_view{},
                                    [](ResponseHeaderMap& headers) {
                                      headers.setReferenceContentType("text/plain; charset=utf-8");
                                    },
                                    absl::nullopt, "http_to_kafka_produce_ok");

  return FilterDataStatus::StopIterationNoBuffer;
}

void HttpToKafkaDecoderFilter::setDecoderFilterCallbacks(StreamDecoderFilterCallbacks& callbacks) {
  decoder_callbacks_ = &callbacks;
  dispatcher_ = &callbacks.dispatcher();
  ENVOY_LOG_MISC(trace, "http_to_kafka setDecoderFilterCallbacks");
}

KafkaStreamManager::ClientId KafkaStreamManager::subscribe(const std::string& topic, Callback cb) {
  std::lock_guard<std::mutex> lock(mutex_);

  auto& state = topics_[topic];
  if (!state.running) {
    startTopicLocked(topic, state);
  }
  if (!state.running || state.consumer_id == 0) {
    ENVOY_LOG_MISC(warn, "http_to_kafka subscribe: failed to start topic={}", topic);
    return 0;
  }

  ClientId id = next_id_++;
  state.clients.push_back({id, std::move(cb)});
  ENVOY_LOG_MISC(trace, "http_to_kafka subscribe: topic={} client_id={} total_clients={}",
                 topic, id, state.clients.size());
  return id;
}

void KafkaStreamManager::unsubscribe(KafkaStreamManager::ClientId id) {
  std::lock_guard<std::mutex> lock(mutex_);

  for (auto it = topics_.begin(); it != topics_.end(); ++it) {
    auto& state = it->second;
    auto& clients = state.clients;

    clients.erase(std::remove_if(clients.begin(), clients.end(),
                  [id](const ClientState& c){ return c.id == id; }),
                  clients.end());

    if (clients.empty()) {
      ENVOY_LOG_MISC(trace, "http_to_kafka unsubscribe: topic={} last_client_removed", it->first);
      stopTopicLocked(it->first, state);
      topics_.erase(it);
      break;
    }
  }
}

void KafkaStreamManager::startTopicLocked(const std::string& topic, TopicState& state) {
  state.running = true;
  state.consumer_id = create_consumer(reinterpret_cast<const uint8_t*>(topic.data()),
                                      topic.size());
  if (state.consumer_id == 0) {
    state.running = false;
    ENVOY_LOG_MISC(warn, "http_to_kafka startTopicLocked: create_consumer failed topic={}", topic);
    return;
  }
  ENVOY_LOG_MISC(trace, "http_to_kafka startTopicLocked: topic={} consumer_id={}",
                 topic, state.consumer_id);

  state.consumer_thread = std::thread([this, topic]() {
    ENVOY_LOG_MISC(trace, "http_to_kafka consumer_thread: start topic={}", topic);
    while (true) {
      {
        std::lock_guard<std::mutex> lock(mutex_);
        if (!topics_.count(topic) || !topics_[topic].running) {
          ENVOY_LOG_MISC(trace, "http_to_kafka consumer_thread: stopping topic={}", topic);
          break;
        }
      }

      uint64_t consumer_id = 0;
      {
        std::lock_guard<std::mutex> lock(mutex_);
        auto it = topics_.find(topic);
        if (it != topics_.end()) {
          consumer_id = it->second.consumer_id;
        }
      }
      if (consumer_id == 0) {
        continue;
      }

      std::string msg = poll_kafka_message(consumer_id, 100);
      if (msg.empty()) {
        continue;
      }

      std::vector<Callback> callbacks;
      {
        std::lock_guard<std::mutex> lock(mutex_);
        auto it = topics_.find(topic);
        if (it == topics_.end()) {
          continue;
        }
        callbacks.reserve(it->second.clients.size());
        for (auto& c : it->second.clients) {
          callbacks.push_back(c.cb);
        }
      }

      for (auto& cb : callbacks) {
        cb(msg);
      }
    }
  });
}

void KafkaStreamManager::stopTopicLocked(const std::string&, TopicState& state) {
  ENVOY_LOG_MISC(trace, "http_to_kafka stopTopicLocked: consumer_id={}", state.consumer_id);
  state.running = false;
  if (state.consumer_id != 0) {
    destroy_consumer(state.consumer_id);
    state.consumer_id = 0;
  }
  if (state.consumer_thread.joinable()) {
    state.consumer_thread.join();
  }
}

KafkaStreamManager& KafkaStreamManager::instance() {
  static KafkaStreamManager mgr;
  return mgr;
}

} // namespace Http
} // namespace Envoy
