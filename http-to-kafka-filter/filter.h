#pragma once

#include <string>
#include "source/common/buffer/buffer_impl.h"
#include "source/extensions/filters/http/common/pass_through_filter.h"
#include "http-to-kafka-filter/filter.pb.h"

namespace Envoy {
namespace Http {

class KafkaStreamManager {
  public:
    using ClientId = uint64_t;
    using Callback = std::function<void(const std::string&)>;
  
    static KafkaStreamManager& instance();
  
    ClientId subscribe(const std::string& topic, Callback cb);
    void unsubscribe(ClientId id);
  
  private:
    struct ClientState {
      ClientId id;
      Callback cb;
    };
    
    struct TopicState {
      std::vector<ClientState> clients;
      std::thread consumer_thread;
      std::atomic<bool> running{false};
      uint64_t consumer_id{0};
    };
    
    void startTopicLocked(const std::string& topic, TopicState& state);
    void stopTopicLocked(const std::string& topic, TopicState& state);

    std::unordered_map<std::string, TopicState> topics_;
    std::atomic<ClientId> next_id_{1};
    std::mutex mutex_;
};

class HttpToKafkaDecoderFilterConfig {
public:
  HttpToKafkaDecoderFilterConfig(const kafkafilter::HttpToKafka& proto_config);

  const std::string& bootstrapServers() const { return bootstrap_servers_; }

  const std::string& actionHeader() const { return action_header_; }
  const std::string& topicHeader() const { return topic_header_; }

  uint32_t maxPayloadBytes() const { return max_payload_bytes_; }

private:
  const std::string bootstrap_servers_;

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
  KafkaStreamManager::ClientId client_id_{0};

  std::atomic<bool> consuming_{false};
  std::thread consumer_thread_;
  Event::Dispatcher* dispatcher_{nullptr};
};

} // namespace Http
} // namespace Envoy
