#include <string>

#include "envoy/registry/registry.h"
#include "envoy/server/filter_config.h"
#include "envoy/http/header_map.h"

#include "http-to-kafka-filter/filter.pb.h"
#include "http-to-kafka-filter/filter.pb.validate.h"
#include "filter.h"

namespace Envoy {
namespace Server {
namespace Configuration {

class HttpSampleToKafkaFilterConfigFactory : public NamedHttpFilterConfigFactory {
public:
  absl::StatusOr<Http::FilterFactoryCb> createFilterFactoryFromProto(const Protobuf::Message& proto_config,
                                                     const std::string&,
                                                     FactoryContext& context) override {

    return createFilter(Envoy::MessageUtil::downcastAndValidate<const kafkafilter::HttpToKafka&>(
                            proto_config, context.messageValidationVisitor()),
                        context);
  }

  ProtobufTypes::MessagePtr createEmptyConfigProto() override {
    return ProtobufTypes::MessagePtr{new kafkafilter::HttpToKafka()};
  }

  std::string name() const override { return "kafkafilter"; }

private:
  Http::FilterFactoryCb createFilter(const kafkafilter::HttpToKafka& proto_config, FactoryContext&) {
    Http::HttpToKafkaDecoderFilterConfigSharedPtr config =
        std::make_shared<Http::HttpToKafkaDecoderFilterConfig>(
            Http::HttpToKafkaDecoderFilterConfig(proto_config));

    return [config](Http::FilterChainFactoryCallbacks& callbacks) -> void {
      auto filter = new Http::HttpToKafkaDecoderFilter(config);
      callbacks.addStreamDecoderFilter(Http::StreamDecoderFilterSharedPtr{filter});
    };
  }
};

static Registry::RegisterFactory<HttpSampleToKafkaFilterConfigFactory, NamedHttpFilterConfigFactory>
    register_;

} // namespace Configuration
} // namespace Server
} // namespace Envoy
