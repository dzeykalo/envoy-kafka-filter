load("@rules_rust//rust:defs.bzl", "rust_static_library")

package(default_visibility = ["//visibility:public"])

rust_static_library(
    name = "kafka_proxy_rust",
    srcs = ["src/lib.rs"],
    crate_root = "src/lib.rs",
    edition = "2021",
)