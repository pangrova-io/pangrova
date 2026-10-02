// Copyright (c) Pangrova contributors. All rights reserved.
// Licensed under the Apache License, Version 2.0.

// FlightServerRegistry: Thread-safe singleton managing FlightResultServer
// and the shared multiplexed FlightShuffleServer instance.

#include "jni/command/FlightServerRegistry.h"

#include <algorithm>
#include <filesystem>
#include <stdexcept>
#include <unistd.h>

#include "jni/RuntimeInitializer.h"
#include "result/FlightResultServer.h"
#include "shuffle/FlightShuffleServer.h"

namespace pangrova::jni {

namespace {

void requireTransportSecurityInvariant(
    bool enabled,
    const std::string& certificate_chain_path,
    const std::string& private_key_path,
    const std::string& root_ca_path,
    const std::string& tls_identity) {
  const bool has_any_value = !certificate_chain_path.empty() ||
      !private_key_path.empty() || !root_ca_path.empty() ||
      !tls_identity.empty();
  if (!enabled && has_any_value) {
    throw std::invalid_argument(
        "Transport TLS settings require enabled=true");
  }
  if (enabled && (certificate_chain_path.empty() ||
                  private_key_path.empty() || root_ca_path.empty() ||
                  tls_identity.empty())) {
    throw std::invalid_argument(
        "Enabled transport TLS requires certificate, private key, root CA and identity");
  }
}

}  // namespace

FlightServerRegistry& FlightServerRegistry::instance() {
  static FlightServerRegistry registry;
  return registry;
}

pangrova::result::FlightResultServer* FlightServerRegistry::resultServer() {
  std::lock_guard<std::mutex> lock(result_mutex_);
  return result_server_.get();
}

void FlightServerRegistry::setResultServer(
    std::unique_ptr<pangrova::result::FlightResultServer> server) {
  std::lock_guard<std::mutex> lock(result_mutex_);
  result_server_ = std::move(server);
}

void FlightServerRegistry::shutdownResultServer() {
  std::lock_guard<std::mutex> lock(result_mutex_);
  if (result_server_) {
    result_server_->stop();
    result_server_.reset();
  }
}

pangrova::shuffle::FlightShuffleServer* FlightServerRegistry::shuffleServer() {
  std::lock_guard<std::mutex> lock(shuffle_mutex_);
  return shuffle_server_.get();
}

int FlightServerRegistry::ensureShuffleServerRunning() {
  std::lock_guard<std::mutex> lock(shuffle_mutex_);
  if (shuffle_server_ && shuffle_server_->isRunning()) {
    return shuffle_server_->boundPort();
  }
  return startShuffleServerLocked();
}

int FlightServerRegistry::startShuffleServerLocked() {
  if (hmac_current_key_.empty()) {
    throw std::runtime_error(
        "Refusing to start Native shuffle without an HMAC ticket key");
  }
  if (!shuffle_limits_) {
    throw std::runtime_error(
        "Refusing to start Native shuffle without a prebooked service budget");
  }
  pangrova::shuffle::FlightShuffleServerConfig config;
  config.reuse_port = true;
  config.buffer_limits = *shuffle_limits_;
  config.hmac_current_key = hmac_current_key_;
  config.hmac_previous_key = hmac_previous_key_;
  {
    std::lock_guard<std::mutex> security_lock(security_mutex_);
    if (transport_tls_enabled_) {
      config.tls_cert_chain_path = transport_tls_certificate_chain_path_;
      config.tls_private_key_path = transport_tls_private_key_path_;
      config.tls_root_ca_path = transport_tls_root_ca_path_;
    }
  }
  shuffle_server_ = std::make_unique<pangrova::shuffle::FlightShuffleServer>(config);
  return shuffle_server_->start();
}

void FlightServerRegistry::registerShuffleHandle(
    int64_t handle,
    pangrova::shuffle::SerializedShuffleQueue* queue,
    const std::string& query_id,
    int32_t stage_id,
    int32_t partition_count,
    uint64_t plan_epoch,
    uint32_t attempt_id,
    uint64_t fencing_token) {
  std::lock_guard<std::mutex> lock(shuffle_mutex_);
  if (!shuffle_server_ || !shuffle_server_->isRunning()) {
    startShuffleServerLocked();
  }
  shuffle_server_->registerHandle(
      handle, queue, query_id, stage_id, partition_count,
      plan_epoch, attempt_id, fencing_token);
}

void FlightServerRegistry::unregisterShuffleHandle(int64_t handle) {
  std::lock_guard<std::mutex> lock(shuffle_mutex_);
  if (shuffle_server_) {
    shuffle_server_->unregisterHandle(handle);
  }
}

void FlightServerRegistry::shutdownShuffleServer() {
  std::lock_guard<std::mutex> lock(shuffle_mutex_);
  if (shuffle_server_) {
    shuffle_server_->stop();
    shuffle_server_.reset();
  }
}

void FlightServerRegistry::shutdownAll() {
  std::scoped_lock lock(result_mutex_, shuffle_mutex_);
  if (result_server_) {
    result_server_->stop();
    result_server_.reset();
  }
  if (shuffle_server_) {
    shuffle_server_->stop();
    shuffle_server_.reset();
  }
}

void FlightServerRegistry::configureShuffleResources(
    const NativeRuntimeConfig& config) {
  std::lock_guard<std::mutex> lock(shuffle_mutex_);
  if (shuffle_server_) {
    throw std::logic_error("Native shuffle limits cannot change while the server exists");
  }
  // An incomplete RuntimeInitConfig may initialize non-shuffle Native APIs,
  // but it cannot advertise or start the shuffle service.
  const bool complete = config.shuffle_retention_bytes > 0 &&
      config.shuffle_transit_bytes > 0 && config.shuffle_spill_bytes > 0 &&
      config.shuffle_spill_files > 0 && !config.shuffle_spill_directory.empty();
  const bool any = config.shuffle_retention_bytes > 0 ||
      config.shuffle_transit_bytes > 0 || config.shuffle_spill_bytes > 0 ||
      config.shuffle_spill_files > 0 || !config.shuffle_spill_directory.empty();
  if (!complete) {
    if (any) throw std::invalid_argument("Native shuffle service budget is incomplete");
    shuffle_limits_.reset();
    return;
  }
  const std::filesystem::path path(config.shuffle_spill_directory);
  if (!path.is_absolute() || !std::filesystem::is_directory(path) ||
      ::access(path.c_str(), W_OK | X_OK) != 0) {
    throw std::invalid_argument(
        "Native shuffle spill directory must be an existing writable absolute directory");
  }
  const auto available = std::filesystem::space(path).available;
  const uint64_t headroom = std::max<uint64_t>(64ULL * 1024 * 1024,
                                               config.shuffle_spill_bytes / 10);
  if (available <= headroom ||
      config.shuffle_spill_bytes > available - headroom) {
    throw std::invalid_argument(
        "Native shuffle spill quota exceeds available disk space after headroom");
  }
  pangrova::shuffle::ShuffleBufferLimits limits;
  limits.memory_bytes = config.shuffle_retention_bytes;
  limits.transit_bytes = config.shuffle_transit_bytes;
  limits.spill_bytes = config.shuffle_spill_bytes;
  limits.spill_files = config.shuffle_spill_files;
  limits.spill_directory = path.string();
  shuffle_limits_ = std::move(limits);
}

void FlightServerRegistry::setHmacKey(std::string currentKey,
                                       std::string previousKey) {
  std::lock_guard<std::mutex> lock(shuffle_mutex_);
  if (currentKey.size() < 32) {
    throw std::invalid_argument(
        "shuffle HMAC key must contain at least 32 bytes");
  }
  if (!previousKey.empty() && previousKey.size() < 32) {
    throw std::invalid_argument(
        "previous shuffle HMAC key must contain at least 32 bytes");
  }
  hmac_current_key_ = std::move(currentKey);
  hmac_previous_key_ = std::move(previousKey);
  if (shuffle_server_ && shuffle_server_->isRunning()) {
    shuffle_server_->setHmacKeys(
        hmac_current_key_, hmac_previous_key_);
  }
}

void FlightServerRegistry::configureTransportSecurity(
    bool enabled,
    std::string certificateChainPath,
    std::string privateKeyPath,
    std::string rootCaPath,
    std::string tlsIdentity) {
  requireTransportSecurityInvariant(
      enabled, certificateChainPath, privateKeyPath, rootCaPath, tlsIdentity);
  std::lock_guard<std::mutex> lock(security_mutex_);
  transport_tls_enabled_ = enabled;
  transport_tls_certificate_chain_path_ = std::move(certificateChainPath);
  transport_tls_private_key_path_ = std::move(privateKeyPath);
  transport_tls_root_ca_path_ = std::move(rootCaPath);
  transport_tls_identity_ = std::move(tlsIdentity);
}

void FlightServerRegistry::applyTransportSecurity(
    pangrova::result::FlightResultServerConfig& config) const {
  std::lock_guard<std::mutex> lock(security_mutex_);
  if (!transport_tls_enabled_) {
    return;
  }
  config.tls_cert_chain_path = transport_tls_certificate_chain_path_;
  config.tls_private_key_path = transport_tls_private_key_path_;
  config.tls_root_ca_path = transport_tls_root_ca_path_;
}

}  // namespace pangrova::jni
