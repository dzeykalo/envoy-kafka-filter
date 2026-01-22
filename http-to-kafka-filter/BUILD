# load(
#     "@envoy//bazel:envoy_build_system.bzl",
#     "envoy_cc_binary",
#     "envoy_cc_library",
#     "envoy_cc_test",
# )
# load("@envoy_api//bazel:api_build_system.bzl", "api_proto_package")

# package(default_visibility = ["//visibility:public"])

load(
    "@envoy//bazel:envoy_build_system.bzl",
    "envoy_cc_contrib_extension",
    "envoy_cc_library",
    "envoy_contrib_package",
    "envoy_cc_test",
)

envoy_contrib_package()

genrule(
    name = "libkafka_proxy_a",
    srcs = [
        "//http-to-kafka-filter/kafka-proxy:Cargo.toml",
        "//http-to-kafka-filter/kafka-proxy:Cargo.lock",
        "//http-to-kafka-filter/kafka-proxy:srcs",
    ],
    outs = ["libkafka_proxy.a"],
    cmd = """
      set -euo pipefail
      CRATE_MANIFEST=$(location //http-to-kafka-filter/kafka-proxy:Cargo.toml)
      CRATE_DIR=$$(dirname $$CRATE_MANIFEST)
      cargo build --release --manifest-path $$CRATE_MANIFEST
      mkdir -p $(@D)
      cp -f $$CRATE_DIR/target/release/libkafka_proxy.a $@
    """,
)

cc_import(
    name = "kafka_proxy_lib",
    static_library = ":libkafka_proxy_a",
    visibility = ["//visibility:public"],
)

# envoy_cc_binary(
#     name = "envoy",
#     repository = "@envoy",
#     deps = [
#         ":http_to_kafka_filter_config",
#         "@envoy//source/exe:envoy_main_entry_lib",
#     ],
# )

# api_proto_package()

envoy_cc_library(
    name = "http_to_kafka_filter_lib",
    srcs = ["filter.cc"],
    hdrs = ["filter.h"],
    repository = "@envoy",
    deps = [
        "//source/common/buffer:buffer_lib",
        "@envoy_api//contrib/envoy/extensions/filters/http/http_to_kafka_filter/v3:pkg_cc_proto",
        "@envoy//source/extensions/filters/http/common:pass_through_filter_lib",
        ":kafka_proxy_lib",
    ],
)

envoy_cc_contrib_extension(
    name = "config",
    srcs = ["config.cc"],
    repository = "@envoy",
    deps = [
        ":http_to_kafka_filter_lib",
        "@envoy//envoy/server:filter_config_interface",
    ],
)

envoy_cc_test(
    name = "integration_test",
    srcs = ["integration_test.cc"],
    repository = "@envoy",
    deps = [
        ":config",
        "@envoy//test/integration:http_integration_lib",
    ],
)
