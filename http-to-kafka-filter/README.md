# Envoy filter example

This project implements a hybrid HTTP-to-Kafka proxy as a custom filter for Envoy Proxy. 
The solution leverages a unique architecture where high-performance C++ network processing in Envoy
is combined with safe Kafka client logic written in Rust, 
bridged via a Foreign Function Interface (FFI). The filter translates HTTP requests into Kafka messages and back.

## Building

To build the Envoy static binary:

1. `git submodule update --init`
2. `bazel build //http-to-kafka-filter:envoy`

## Testing

To run the `http-to-kafka-filter` integration test:

`bazel test //http-to-kafka-filter:http_to_kafka_filter_integration_test`

## How it works

See the [network filter example](../README.md#how-it-works).

## Filter example

- The main task is to write a class that implements the interface
 [`Envoy::Http::StreamDecoderFilter`][StreamDecoderFilter] as in
 [`http_to_kafka_filter.h`](http_to_kafka_filter.h) and [`http_to_kafka_filter.cc`](http_to_kafka_filter.cc),
 which contains functions that handle http headers, data, and trailers.

```yaml
http_filters:
- name: http_to_kafka
  typed_config:
    "@type": type.googleapis.com/kafkafilter.HttpToKafka
    kafka_host: localhost
    kafka_port: 9092
    max_payload_bytes: 1048576
- name: envoy.router
  typed_config: {}
```
 

[StreamDecoderFilter]: https://github.com/envoyproxy/envoy/blob/b2610c84aeb1f75c804d67effcb40592d790e0f1/include/envoy/http/filter.h#L300
[StreamEncoderFilter]: https://github.com/envoyproxy/envoy/blob/b2610c84aeb1f75c804d67effcb40592d790e0f1/include/envoy/http/filter.h#L413
[StreamFilter]: https://github.com/envoyproxy/envoy/blob/b2610c84aeb1f75c804d67effcb40592d790e0f1/include/envoy/http/filter.h#L462
[BUILD]: https://github.com/envoyproxy/envoy-filter-example/blob/d76d3096c4cbd647d26b44b3f801c3afbc81d3e2/http-filter-example/BUILD#L15-L18
[front-envoy.yaml]: https://github.com/envoyproxy/envoy/blob/b2610c84aeb1f75c804d67effcb40592d790e0f1/examples/front-proxy/front-envoy.yaml#L28
