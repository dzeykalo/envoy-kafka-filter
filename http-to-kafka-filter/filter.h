#pragma once

#include <string>
#include "source/common/buffer/buffer_impl.h"
#include "source/extensions/filters/http/common/pass_through_filter.h"
#include "contrib/envoy/extensions/filters/http/http_to_kafka_filter/v3/http_to_kafka_filter.pb.h"

namespace Envoy {
namespace Http {

namespace kafkafilter = envoy::extensions::filters::http::http_to_kafka_filter::v3;

class HttpToKafkaDecoderFilterConfig {
public:
  HttpToKafkaDecoderFilterConfig(const kafkafilter::HttpToKafka& proto_config);

  const std::string& kafkaHost() const { return kafka_host_; }
  uint32_t kafkaPort() const { return kafka_port_; }

  const std::string& actionHeader() const { return action_header_; }
  const std::string& topicHeader() const { return topic_header_; }

  uint32_t maxPayloadBytes() const { return max_payload_bytes_; }

private:
  const std::string kafka_host_;
  const uint32_t kafka_port_;

  const std::string action_header_;
  const std::string topic_header_;

  const uint32_t max_payload_bytes_;
};

using HttpToKafkaDecoderFilterConfigSharedPtr = std::shared_ptr<HttpToKafkaDecoderFilterConfig>;

class HttpToKafkaDecoderFilter : public PassThroughDecoderFilter {
public:
  HttpToKafkaDecoderFilter(HttpToKafkaDecoderFilterConfigSharedPtr);
  ~HttpToKafkaDecoderFilter() override = default;

  void onDestroy() override;

  FilterHeadersStatus decodeHeaders(RequestHeaderMap&, bool end_stream) override;
  FilterDataStatus decodeData(Buffer::Instance&, bool end_stream) override;
  void setDecoderFilterCallbacks(StreamDecoderFilterCallbacks& callbacks) override;

private:
  enum class Mode { None, Produce, Consume };

  void sendError(Code code, absl::string_view msg, absl::string_view details);
  void ensureKafkaConfiguredOnce();

  const HttpToKafkaDecoderFilterConfigSharedPtr config_;
  StreamDecoderFilterCallbacks* decoder_callbacks_{nullptr};

  Mode mode_{Mode::None};
  std::string topic_;
  Buffer::OwnedImpl body_;
  bool replied_{false};

  std::atomic<bool> consuming_{false};
  std::thread consumer_thread_;
  Event::Dispatcher* dispatcher_{nullptr};
};

class KafkaStreamManager {
  public:
    using ClientId = uint64_t;
    using Callback = std::function<void(const std::string&)>;
  
    static KafkaStreamManager& instance();
  
    ClientId subscribe(const std::string& topic, Callback cb);
    void unsubscribe(ClientId id);
  
  private:
    struct TopicState {
      std::vector<std::pair<ClientId, Callback>> clients;
      std::thread consumer_thread;
      std::atomic<bool> running{false};
    };
  
    std::mutex mutex_;
    std::unordered_map<std::string, TopicState> topics_;
 };

} // namespace Http
} // namespace Envoy
