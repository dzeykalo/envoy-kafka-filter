#include "test/integration/http_integration.h"

namespace Envoy {
class HttpToKafkaFilterIntegrationTest : public HttpIntegrationTest,
                                        public testing::TestWithParam<Network::Address::IpVersion> {
public:
  HttpToKafkaFilterIntegrationTest()
      : HttpIntegrationTest(Http::CodecClient::Type::HTTP1, GetParam()) {}
  /**
   * Initializer for an individual integration test.
   */
  void SetUp() override { initialize(); }

  void initialize() override {
    config_helper_.prependFilter(
        "{ name: http_to_kafka, typed_config: { \"@type\": type.googleapis.com/kafkafilter.HttpToKafka, "
        "kafka_host: kafka, "
        "kafka_port: 9092, "
        "action_header: \"x-kafka-action\", "
        "topic_header: \"x-kafka-topic\", "
        "max_payload_bytes: 1048576 } }");
    HttpIntegrationTest::initialize();
  }
};

INSTANTIATE_TEST_SUITE_P(IpVersions, HttpToKafkaFilterIntegrationTest,
                         testing::ValuesIn(TestEnvironment::getIpVersionsForTest()));

TEST_P(HttpToKafkaFilterIntegrationTest, PassThroughWhenNoKafkaHeaders) {
  Http::TestRequestHeaderMapImpl headers{
      {":method", "GET"}, {":path", "/"}, {":authority", "host"}};
  Http::TestRequestHeaderMapImpl response_headers{
      {":status", "200"}};

  IntegrationCodecClientPtr codec_client;
  FakeHttpConnectionPtr fake_upstream_connection;
  FakeStreamPtr request_stream;

  codec_client = makeHttpConnection(lookupPort("http"));
  auto response = codec_client->makeHeaderOnlyRequest(headers);
  ASSERT_TRUE(fake_upstreams_[0]->waitForHttpConnection(*dispatcher_, fake_upstream_connection));
  ASSERT_TRUE(fake_upstream_connection->waitForNewStream(*dispatcher_, request_stream));
  ASSERT_TRUE(request_stream->waitForEndStream(*dispatcher_));
  
  request_stream->encodeHeaders(response_headers, true);
  ASSERT_TRUE(response->waitForEndStream());
  EXPECT_EQ("200", response->headers().getStatusValue());

  codec_client->close();
}

TEST_P(HttpToKafkaFilterIntegrationTest, ProduceHeaderOnlyIsLocalReply_NoUpstream) {
  Http::TestRequestHeaderMapImpl headers{
      {":method", "POST"},
      {":path", "/"},
      {":authority", "host"},
      {"x-kafka-action", "produce"},
      {"x-kafka-topic", "test0"},
  };

  IntegrationCodecClientPtr codec_client = makeHttpConnection(lookupPort("http"));
  auto response = codec_client->makeHeaderOnlyRequest(headers);

  ASSERT_TRUE(response->waitForEndStream());
  // У вас в фильтре produce+end_stream => "empty body" => 400
  EXPECT_EQ("400", response->headers().getStatusValue());

  FakeHttpConnectionPtr fake_upstream_connection;
  EXPECT_FALSE(fake_upstreams_[0]->waitForHttpConnection(*dispatcher_, fake_upstream_connection,
                                                         std::chrono::milliseconds(50)));

  codec_client->close();
}
} // namespace Envoy
