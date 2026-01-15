load(
    "@envoy//bazel:envoy_build_system.bzl",
    "envoy_cc_binary",
    "envoy_cc_library",
    "envoy_cc_test",
)
load("@envoy_api//bazel:api_build_system.bzl", "api_proto_package")

package(default_visibility = ["//visibility:public"])

cc_import(
    name = "kafka_proxy_lib",
    static_library = "//http-to-kafka-filter/kafka-proxy:kafka_proxy_rust",
    visibility = ["//visibility:public"],
)

envoy_cc_binary(
    name = "envoy",
    repository = "@envoy",
    deps = [
        ":http_to_kafka_filter_config",
        "@envoy//source/exe:envoy_main_entry_lib",
    ],
)

api_proto_package()

envoy_cc_library(
    name = "http_to_kafka_filter_lib",
    srcs = ["http_to_kafka_filter.cc"],
    hdrs = ["http_to_kafka_filter.h"],
    repository = "@envoy",
    deps = [
        ":pkg_cc_proto",
        "@envoy//source/extensions/filters/http/common:pass_through_filter_lib",
        ":kafka_proxy_lib",
    ],
)

envoy_cc_library(
    name = "http_to_kafka_filter_config",
    srcs = ["http_to_kafka_filter_config.cc"],
    repository = "@envoy",
    deps = [
        ":http_to_kafka_filter_lib",
        "@envoy//envoy/server:filter_config_interface",
    ],
)

envoy_cc_test(
    name = "http_to_kafka_filter_integration_test",
    srcs = ["http_to_kafka_filter_integration_test.cc"],
    repository = "@envoy",
    deps = [
        ":http_to_kafka_filter_config",
        "@envoy//test/integration:http_integration_lib",
    ],
)
