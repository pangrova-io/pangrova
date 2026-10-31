// Copyright (c) Pangrova contributors. All rights reserved.
// Licensed under the Apache License, Version 2.0.

// FlightServerRegistry: Manages lifecycle of per-worker FlightResultServer
// and the shared multiplexed FlightShuffleServer instance.
//
// The shuffle server is a single instance per worker that multiplexes all
// executor handles on one port, eliminating per-handle port allocation.
//
// @ThreadSafe — metadata is mutex-guarded; shuffle lifecycle operations retain
// the actual shared owner and perform network initialization/drain outside locks.
//
// Placement: jni/command/ — JNI command infrastructure.
#pragma once

#include <cstdint>
#include <condition_variable>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <utility>

// Forward declarations to avoid pulling heavy headers.
namespace pangrova::result { class FlightResultServer; }
namespace pangrova::result { struct FlightResultServerConfig; }
namespace pangrova::shuffle { class FlightShuffleServer; }
namespace pangrova::shuffle { struct FlightShuffleServerConfig; }

// SerializedShuffleQueue is a type alias (not a class), so it cannot be
// forward-declared. Include the lightweight header directly.
#include "engine/shuffle/SerializedShuffleQueue.h"
#include "engine/shuffle/ShuffleBufferStore.h"
#include "engine/shuffle/ShuffleControlGate.h"
#include "engine/shuffle/ShuffleProducerRegistration.h"

namespace pangrova::jni {

struct NativeRuntimeConfig;

struct ShuffleRegistrationLease {
  ShuffleRegistrationLease(std::shared_ptr<pangrova::shuffle::FlightShuffleServer> actual_server,
      pangrova::shuffle::ShuffleControlGate::Lease actual_control,
      std::shared_ptr<pangrova::shuffle::ShuffleProducerRegistration> actual_registration,
      int actual_port)
      : server(std::move(actual_server)), control(std::move(actual_control)),
        registration(std::move(actual_registration)), bound_port(actual_port) {}
  ShuffleRegistrationLease(ShuffleRegistrationLease&&) noexcept = default;
  // Assignment would replace the service before relinquishing its old lease,
  // potentially making that service's destructor wait for this very object.
  ShuffleRegistrationLease& operator=(ShuffleRegistrationLease&&) = delete;
  std::shared_ptr<pangrova::shuffle::FlightShuffleServer> server;
  pangrova::shuffle::ShuffleControlGate::Lease control;
  std::shared_ptr<pangrova::shuffle::ShuffleProducerRegistration> registration;
  int bound_port;
};

class FlightServerRegistry {
 public:
  static FlightServerRegistry& instance();

  // Result server (per-worker singleton).
  std::shared_ptr<pangrova::result::FlightResultServer> resultServer();
  int ensureResultServerRunning(pangrova::result::FlightResultServerConfig config);
  void shutdownResultServer();

  // Shared shuffle server (per-worker singleton, multiplexes all handles).
  // Ensures the shared shuffle server is running. Returns bound port.
  int ensureShuffleServerRunning();

  // Register an executor handle with the shared shuffle server.
  ShuffleRegistrationLease registerShuffleHandle(int64_t handle,
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

  // Seals and drains the shuffle owner before shutting down the result server.
  void shutdownAll();

  // Immutable per-runtime service limits, configured before any task can
  // start the Flight server. Reinitialization replaces them only after stop.
  void configureShuffleResources(const NativeRuntimeConfig& config);
  // Only the successful runtime initializer opens a sealed service cohort.
  void activateShuffleRuntime();

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
  std::shared_ptr<pangrova::shuffle::FlightShuffleServer> acquireShuffleServer();
  std::shared_ptr<pangrova::shuffle::FlightShuffleServer> startShuffleService(
      pangrova::shuffle::FlightShuffleServerConfig config);

  mutable std::mutex result_mutex_;
  std::shared_ptr<pangrova::result::FlightResultServer> result_server_;
  enum class ResultServiceState { STOPPED, STARTING, READY, CLOSING };
  ResultServiceState result_state_ = ResultServiceState::STOPPED;
  std::condition_variable result_cv_;

  mutable std::mutex shuffle_mutex_;
  std::shared_ptr<pangrova::shuffle::FlightShuffleServer> shuffle_server_;
  enum class ShuffleServiceState { STOPPED, STARTING, READY, CLOSING };
  ShuffleServiceState shuffle_state_ = ShuffleServiceState::STOPPED;
  std::condition_variable shuffle_cv_;
  bool shuffle_runtime_sealed_ = true;
  std::optional<pangrova::shuffle::ShuffleBufferLimits> shuffle_limits_;

  // HMAC key(s) for shuffle ticket verification. Protected by shuffle_mutex_.
  std::string hmac_current_key_;
  std::string hmac_previous_key_;
  uint64_t hmac_revision_ = 0;
  // Serialize rotations through actual verifier application without retaining
  // the registry metadata mutex over another owner's operation.
  std::mutex hmac_update_mutex_;

  mutable std::mutex security_mutex_;
  bool transport_tls_enabled_ = false;
  std::string transport_tls_certificate_chain_path_;
  std::string transport_tls_private_key_path_;
  std::string transport_tls_root_ca_path_;
  std::string transport_tls_identity_;
};

}  // namespace pangrova::jni
