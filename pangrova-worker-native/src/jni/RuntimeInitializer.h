// Copyright (c) Pangrova contributors. All rights reserved.
// Licensed under the Apache License, Version 2.0.

// RuntimeInitializer: Initializes and tears down the native execution runtime.
// Manages Velox memory, functions, file systems, cache, and connector registration.
// C1 compliance: file renamed from VeloxRuntimeInitializer — not a direct Velox SPI impl.

#pragma once

#include <jni.h>
#include <cstdint>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

// Forward declarations to avoid pulling heavy headers.
namespace folly { class Executor; }
namespace facebook::velox::cache { class AsyncDataCache; }
namespace pangrova::velox { class VeloxMemoryGovernor; }
namespace pangrova::velox { class TableFunctionRegistry; }
namespace pangrova::udf { class NativeUdfRegistry; }
namespace pangrova::memory {
class BlockAllocationListener;
class PangrovaAllocationListener;
}  // namespace pangrova::memory

namespace pangrova::jni {

// Parsed native engine configuration from NativeConfig protobuf.
struct NativeRuntimeConfig {
  int64_t max_memory_bytes = 8L * 1024 * 1024 * 1024;
  int32_t io_thread_count = 4;
  bool cache_enabled = false;
  int64_t ram_cache_bytes = 8L * 1024 * 1024 * 1024;
  int64_t ssd_cache_bytes = 100L * 1024 * 1024 * 1024;
  std::string ssd_cache_path;
  int64_t artifact_cache_bytes = 0;
  std::string artifact_cache_path;
  int64_t artifact_index_memory_bytes = 0;
  // Spill configuration.
  bool spill_enabled = false;
  std::string spill_directory;
  int64_t max_spill_bytes = 0;  // 0 = unlimited
  // Native Flight shuffle service is not started unless all of these are
  // explicitly supplied by the Worker and prebooked in its shared arbiter.
  uint64_t shuffle_retention_bytes = 0;
  uint64_t shuffle_transit_bytes = 0;
  uint64_t shuffle_spill_bytes = 0;
  uint32_t shuffle_spill_files = 0;
  std::string shuffle_spill_directory;
  // Parallel execution.
  int32_t num_drivers = 1;
  // Configurable thread pool sizes (0 = use computed defaults).
  int32_t executor_task_threads = 0;
  int32_t executor_driver_threads = 0;

  // P3-BUG-18: Non-JVM thread memory cap (bytes). When > 0, overrides the
  // default 512MB hard cap for allocations from non-JVM-attached threads.
  int64_t non_jvm_memory_cap_bytes = 0;  // 0 = use default (512MB)

  // SharedArbitrator reserved capacity.
  int64_t arbitrator_reserved_capacity = 0;
  int64_t query_initial_capacity = 256L * 1024 * 1024;  // 256MB

  // Velox FileSystem authentication configuration.
  std::string s3_endpoint;
  std::string s3_access_key;
  std::string s3_secret_key;
  std::string s3_region;
  std::string hdfs_host;
  int32_t hdfs_port = 0;
  std::string kerberos_principal;
  std::string kerberos_keytab;
  std::string gcs_credentials_path;

  // OSS S3-compatible access configuration.
  bool s3_path_style_access = false;
  bool s3_ssl_enabled = true;  // proto3 bool defaults false; C++ defaults true
  std::string s3_security_token;

  // Memory pressure thresholds (percentage 0-100). 0 → use defaults.
  double pressure_threshold_low = 0.0;
  double pressure_threshold_medium = 0.0;
  double pressure_threshold_high = 0.0;
  double pressure_threshold_critical = 0.0;

  // Mutual TLS for all Native data-plane servers and clients.
  bool transport_tls_enabled = false;
  std::string transport_tls_certificate_chain_path;
  std::string transport_tls_private_key_path;
  std::string transport_tls_root_ca_path;
  std::string transport_tls_identity;
};

// Parses NativeConfig protobuf bytes into NativeRuntimeConfig.
// Returns defaults when data is empty. Throws when a non-empty payload is not
// a valid canonical v2 RuntimeInitConfig or violates a security invariant.
NativeRuntimeConfig parseNativeConfig(const std::vector<uint8_t>& data);

// Initializes the full native runtime: memory, file systems,
// functions, and optionally AsyncDataCache.
//
// allocationListener: Java AllocationListener jobject for memory coordination.
// Must be called from a JNI-attached thread.
void initializeVeloxRuntime(const NativeRuntimeConfig& config,
                            JNIEnv* env, jobject allocationListener);

// Test-only overload: initializes runtime without JNI listener chain.
// Uses default SharedArbitrator instead of PangrovaArbitrator.
// Must NOT be used in production — no Java-side memory coordination.
void initializeVeloxRuntime(const NativeRuntimeConfig& config);

// Tears down all global runtime state (called by nativeShutdown).
void shutdownVeloxRuntime();

// Global accessors for runtime components.
velox::VeloxMemoryGovernor* globalVeloxMemoryGovernor();
facebook::velox::cache::AsyncDataCache* globalAsyncDataCache();
udf::NativeUdfRegistry* globalUdfRegistry();
velox::TableFunctionRegistry* globalTableFunctionRegistry();
std::shared_ptr<folly::Executor> globalExecutor();
// Dedicated executor for blocking Native data-plane I/O. It is owned by the
// runtime lifecycle and is drained before Velox memory is destroyed.
std::shared_ptr<folly::Executor> globalIoExecutor();

// Build a config properties map from NativeRuntimeConfig
// for filesystem authentication (S3/HDFS/GCS).
// Used at query time to pass auth config via ConfigBase.
std::unordered_map<std::string, std::string> buildFileSystemConfigMap(
    const NativeRuntimeConfig& config);

}  // namespace pangrova::jni
