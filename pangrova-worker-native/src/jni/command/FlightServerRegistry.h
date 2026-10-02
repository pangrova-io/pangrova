// Copyright (c) Pangrova contributors. All rights reserved.
// Licensed under the Apache License, Version 2.0.

// FlightServerRegistry: Manages lifecycle of per-worker FlightResultServer
// and the shared multiplexed FlightShuffleServer instance.
//
// The shuffle server is a single instance per worker that multiplexes all
// executor handles on one port, eliminating per-handle port allocation.
//
// @ThreadSafe — all public methods are mutex-guarded.
//
// Placement: jni/command/ — JNI command infrastructure.
#pragma once

#include <cstdint>
#include <memory>
#include <mutex>
#include <optional>
#include <string>

// Forward declarations to avoid pulling heavy headers.
namespace pangrova::result { class FlightResultServer; }
namespace pangrova::result { struct FlightResultServerConfig; }
namespace pangrova::shuffle { class FlightShuffleServer; }
namespace pangrova::shuffle { struct FlightShuffleServerConfig; }

// SerializedShuffleQueue is a type alias (not a class), so it cannot be
// forward-declared. Include the lightweight header directly.
#include "shuffle/SerializedShuffleQueue.h"
#include "shuffle/ShuffleBufferStore.h"

namespace pangrova::jni {

struct NativeRuntimeConfig;

class FlightServerRegistry {
 public:
  static FlightServerRegistry& instance();

  // Result server (per-worker singleton).
  pangrova::result::FlightResultServer* resultServer();
  void setResultServer(std::unique_ptr<pangrova::result::FlightResultServer> server);
  void shutdownResultServer();

  // Shared shuffle server (per-worker singleton, multiplexes all handles).
  // Returns the shared server, creating and starting it if needed.
  pangrova::shuffle::FlightShuffleServer* shuffleServer();

  // Ensures the shared shuffle server is running. Returns bound port.
  int ensureShuffleServerRunning();

  // Register an executor handle with the shared shuffle server.
  void registerShuffleHandle(int64_t handle,
                             pangrova::shuffle::SerializedShuffleQueue* queue,
                             const std::string& query_id,
                             int32_t stage_id,
                             int32_t partition_count,
                             uint64_t plan_epoch,
                             uint32_t attempt_id,
                             uint64_t fencing_token);

  // Unregister an executor handle from the shared shuffle server.
  void unregisterShuffleHandle(int64_t handle);

  // Shuts down the shared shuffle server.
  void shutdownShuffleServer();

  // Shuts down result server and shuffle server atomically.
  void shutdownAll();

  // Immutable per-runtime service limits, configured before any task can
  // start the Flight server. Reinitialization replaces them only after stop.
  void configureShuffleResources(const NativeRuntimeConfig& config);

  // Stores HMAC key(s) for shuffle ticket verification.
  // Called once from JNI nativeSetHmacKey during worker startup.
  // The key is applied to the FlightShuffleServerConfig when the shuffle
  // server is next created or recreated.
  void setHmacKey(std::string currentKey, std::string previousKey);

  // Replaces process-local mutual TLS settings after all existing data-plane
  // servers have been stopped during runtime re-initialization.
  void configureTransportSecurity(
      bool enabled,
      std::string certificateChainPath,
      std::string privateKeyPath,
      std::string rootCaPath,
      std::string tlsIdentity);

  // Applies the immutable process-local transport security snapshot to a
  // result server configuration.
  void applyTransportSecurity(
      pangrova::result::FlightResultServerConfig& config) const;

 private:
  FlightServerRegistry() = default;
  int startShuffleServerLocked();

  mutable std::mutex result_mutex_;
  std::unique_ptr<pangrova::result::FlightResultServer> result_server_;

  mutable std::mutex shuffle_mutex_;
  std::unique_ptr<pangrova::shuffle::FlightShuffleServer> shuffle_server_;
  std::optional<pangrova::shuffle::ShuffleBufferLimits> shuffle_limits_;

  // HMAC key(s) for shuffle ticket verification. Protected by shuffle_mutex_.
  std::string hmac_current_key_;
  std::string hmac_previous_key_;

  mutable std::mutex security_mutex_;
  bool transport_tls_enabled_ = false;
  std::string transport_tls_certificate_chain_path_;
  std::string transport_tls_private_key_path_;
  std::string transport_tls_root_ca_path_;
  std::string transport_tls_identity_;
};

}  // namespace pangrova::jni
