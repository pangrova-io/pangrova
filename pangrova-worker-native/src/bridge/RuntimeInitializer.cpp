// Copyright (c) Pangrova contributors. All rights reserved.
// Licensed under the Apache License, Version 2.0.

// RuntimeInitializer.cpp — Thin delegation layer.
//
// All runtime state and lifecycle logic now lives in NativeEngineLifecycle
// (Task 4, native-jvm-reuse). The free functions declared in
// RuntimeInitializer.h are preserved for backward compatibility — every
// existing caller (JniBridge.cpp, NativeCommandDispatcher.cpp, tests)
// continues to work without changes.
//
// This file retains only:
//   - parseNativeConfig()       — pure config parsing (no state)
//   - buildFileSystemConfigMap() — pure config builder (no state)
//   - Thin wrappers that delegate to NativeEngineLifecycle::instance()

#include "bridge/RuntimeInitializer.h"
#include "bridge/command/FlightServerRegistry.h"

#include <stdexcept>

#include "engine/velox/runtime/NativeEngineLifecycle.h"

#include "pangrova/protocol/v2/runtime.pb.h"

namespace pangrova::jni {

// ---------------------------------------------------------------------------
// Config parsing (pure function — no state)
// ---------------------------------------------------------------------------

NativeRuntimeConfig parseNativeConfig(const std::vector<uint8_t>& data) {
  NativeRuntimeConfig config;
  if (data.empty()) {
    return config;
  }

  pangrova::protocol::v2::RuntimeInitConfig proto;
  if (!proto.ParseFromArray(data.data(), static_cast<int>(data.size()))) {
    throw std::invalid_argument(
        "Invalid canonical v2 RuntimeInitConfig payload");
  }
  {
    if (proto.max_memory_bytes() > 0) {
      config.max_memory_bytes = proto.max_memory_bytes();
    }
    if (proto.io_thread_count() > 0) {
      config.io_thread_count = proto.io_thread_count();
    }
    config.cache_enabled = proto.cache_enabled();
    if (proto.ram_cache_bytes() > 0) {
      config.ram_cache_bytes = proto.ram_cache_bytes();
    }
    if (proto.ssd_cache_bytes() > 0) {
      config.ssd_cache_bytes = proto.ssd_cache_bytes();
    }
    if (!proto.ssd_cache_path().empty()) {
      config.ssd_cache_path = proto.ssd_cache_path();
    }
    if (proto.artifact_cache_bytes() > 0) {
      config.artifact_cache_bytes = proto.artifact_cache_bytes();
    }
    if (!proto.artifact_cache_path().empty()) {
      config.artifact_cache_path = proto.artifact_cache_path();
    }
    if (proto.artifact_index_memory_bytes() > 0) {
      config.artifact_index_memory_bytes =
          proto.artifact_index_memory_bytes();
    }
    if (config.cache_enabled &&
        (config.artifact_cache_path.empty() ||
         config.artifact_cache_bytes <= 0 ||
         config.artifact_index_memory_bytes <= 0)) {
      throw std::invalid_argument(
          "Enabled Native cache requires positive artifact SSD and index RAM quotas");
    }
    // Parse filesystem authentication fields.
    if (!proto.s3_endpoint().empty()) {
      config.s3_endpoint = proto.s3_endpoint();
    }
    if (!proto.s3_access_key().empty()) {
      config.s3_access_key = proto.s3_access_key();
    }
    if (!proto.s3_secret_key().empty()) {
      config.s3_secret_key = proto.s3_secret_key();
    }
    if (!proto.s3_region().empty()) {
      config.s3_region = proto.s3_region();
    }
    if (!proto.hdfs_host().empty()) {
      config.hdfs_host = proto.hdfs_host();
    }
    if (proto.hdfs_port() > 0) {
      config.hdfs_port = proto.hdfs_port();
    }
    if (!proto.kerberos_principal().empty()) {
      config.kerberos_principal = proto.kerberos_principal();
    }
    if (!proto.kerberos_keytab().empty()) {
      config.kerberos_keytab = proto.kerberos_keytab();
    }
    if (!proto.gcs_credentials_path().empty()) {
      config.gcs_credentials_path = proto.gcs_credentials_path();
    }
    // OSS S3-compatible fields.
    config.s3_path_style_access = proto.s3_path_style_access();
    // Proto3 bool defaults to false. The C++ NativeRuntimeConfig defaults
    // s3_ssl_enabled to true (secure by default). Only disable SSL when the
    // proto explicitly sets s3_ssl_enabled=false AND s3_path_style_access=true,
    // which signals the caller deliberately configured S3 (e.g., local MinIO).
    // Without this guard, a caller that sets s3_endpoint but forgets
    // s3_ssl_enabled would silently downgrade to plaintext HTTP.
    if (proto.s3_ssl_enabled()) {
      config.s3_ssl_enabled = true;
    } else if (!config.s3_endpoint.empty() && proto.s3_path_style_access()) {
      config.s3_ssl_enabled = false;
    }
    if (!proto.s3_security_token().empty()) {
      config.s3_security_token = proto.s3_security_token();
    }
    // Memory pressure thresholds — only override if explicitly set (> 0).
    if (proto.memory_pressure_threshold_low() > 0) {
      config.pressure_threshold_low = proto.memory_pressure_threshold_low();
    }
    if (proto.memory_pressure_threshold_medium() > 0) {
      config.pressure_threshold_medium = proto.memory_pressure_threshold_medium();
    }
    if (proto.memory_pressure_threshold_high() > 0) {
      config.pressure_threshold_high = proto.memory_pressure_threshold_high();
    }
    if (proto.memory_pressure_threshold_critical() > 0) {
      config.pressure_threshold_critical = proto.memory_pressure_threshold_critical();
    }
    // Engine-level spill. The directory is the switch: without a concrete path
    // there is nowhere to spill to, so enabling spill would leave
    // VeloxMemoryGovernor::spillDirectory() empty and silently inert.
    if (!proto.spill_directory().empty()) {
      config.spill_directory = proto.spill_directory();
      config.spill_enabled = proto.spill_enabled();
    }
    config.shuffle_retention_bytes = proto.shuffle_retention_bytes();
    config.shuffle_transit_bytes = proto.shuffle_transit_bytes();
    config.shuffle_spill_bytes = proto.shuffle_spill_bytes();
    config.shuffle_spill_files = proto.shuffle_spill_files();
    config.shuffle_spill_directory = proto.shuffle_spill_directory();

    const auto& security = proto.transport_security();
    config.transport_tls_enabled = security.tls_enabled();
    config.transport_tls_certificate_chain_path =
        security.certificate_chain_path();
    config.transport_tls_private_key_path = security.private_key_path();
    config.transport_tls_root_ca_path = security.root_ca_path();
    config.transport_tls_identity = security.tls_identity();
    const bool has_transport_security_value =
        !config.transport_tls_certificate_chain_path.empty() ||
        !config.transport_tls_private_key_path.empty() ||
        !config.transport_tls_root_ca_path.empty() ||
        !config.transport_tls_identity.empty();
    if (!config.transport_tls_enabled && has_transport_security_value) {
      throw std::invalid_argument(
          "Transport TLS settings require tls_enabled=true");
    }
    if (config.transport_tls_enabled &&
        (config.transport_tls_certificate_chain_path.empty() ||
         config.transport_tls_private_key_path.empty() ||
         config.transport_tls_root_ca_path.empty() ||
         config.transport_tls_identity.empty())) {
      throw std::invalid_argument(
          "Enabled transport TLS requires certificate, private key, root CA and identity");
    }
  }

  return config;
}

// ---------------------------------------------------------------------------
// Filesystem config builder (pure function — no state)
// ---------------------------------------------------------------------------

std::unordered_map<std::string, std::string> buildFileSystemConfigMap(
    const NativeRuntimeConfig& config) {
  std::unordered_map<std::string, std::string> properties;

  // S3 configuration — Velox hive.s3.* keys.
  if (!config.s3_endpoint.empty()) {
    properties["hive.s3.endpoint"] = config.s3_endpoint;
  }
  if (!config.s3_access_key.empty()) {
    properties["hive.s3.aws-access-key"] = config.s3_access_key;
  }
  if (!config.s3_secret_key.empty()) {
    properties["hive.s3.aws-secret-key"] = config.s3_secret_key;
  }
  if (!config.s3_region.empty()) {
    properties["hive.s3.region"] = config.s3_region;
  }

  // HDFS configuration.
  if (!config.hdfs_host.empty()) {
    properties["hive.hdfs.host"] = config.hdfs_host;
  }
  if (config.hdfs_port > 0) {
    properties["hive.hdfs.port"] = std::to_string(config.hdfs_port);
  }

  // Kerberos configuration (shared by HDFS and other Hadoop-based systems).
  if (!config.kerberos_principal.empty()) {
    properties["hive.hdfs.kerberos.principal"] = config.kerberos_principal;
  }
  if (!config.kerberos_keytab.empty()) {
    properties["hive.hdfs.kerberos.keytab"] = config.kerberos_keytab;
  }

  // GCS configuration.
  if (!config.gcs_credentials_path.empty()) {
    properties["hive.gcs.credentials-file"] = config.gcs_credentials_path;
  }

  // OSS S3-compatible configuration.
  if (config.s3_path_style_access) {
    properties["hive.s3.path-style-access"] = "true";
  }
  // Always set SSL explicitly. Proto3 bool defaults to false, so an unset
  // s3_ssl_enabled field means "caller didn't specify" — default to enabled.
  // Only disable when the field is explicitly set to false by the caller.
  properties["hive.s3.ssl.enabled"] =
      config.s3_ssl_enabled ? "true" : "false";
  if (!config.s3_security_token.empty()) {
    properties["hive.s3.security-token"] = config.s3_security_token;
  }

  return properties;
}

// ---------------------------------------------------------------------------
// Thin delegation wrappers — all state lives in NativeEngineLifecycle
// ---------------------------------------------------------------------------

void initializeVeloxRuntime(const NativeRuntimeConfig& config,
                            JNIEnv* env, jobject allocationListener) {
  FlightServerRegistry::instance().shutdownAll();
  NativeEngineLifecycle::instance().initialize(config, env, allocationListener);
  FlightServerRegistry::instance().activateShuffleRuntime();
}

void initializeVeloxRuntime(const NativeRuntimeConfig& config) {
  FlightServerRegistry::instance().shutdownAll();
  NativeEngineLifecycle::instance().initialize(config);
  FlightServerRegistry::instance().activateShuffleRuntime();
}

void shutdownVeloxRuntime() {
  FlightServerRegistry::instance().shutdownAll();
  NativeEngineLifecycle::instance().shutdown();
}

pangrova::velox::VeloxMemoryGovernor* globalVeloxMemoryGovernor() {
  return NativeEngineLifecycle::instance().veloxMemoryGovernor();
}

facebook::velox::cache::AsyncDataCache* globalAsyncDataCache() {
  return NativeEngineLifecycle::instance().asyncDataCache();
}

pangrova::udf::NativeUdfRegistry* globalUdfRegistry() {
  return NativeEngineLifecycle::instance().udfRegistry();
}

pangrova::velox::TableFunctionRegistry* globalTableFunctionRegistry() {
  return NativeEngineLifecycle::instance().tableFunctionRegistry();
}

std::shared_ptr<folly::Executor> globalExecutor() {
  return NativeEngineLifecycle::instance().executor();
}

std::shared_ptr<folly::Executor> globalIoExecutor() {
  return NativeEngineLifecycle::instance().ioExecutor();
}

std::shared_ptr<folly::Executor> globalBlockingIoExecutor() {
  return NativeEngineLifecycle::instance().blockingIoExecutor();
}

}  // namespace pangrova::jni
