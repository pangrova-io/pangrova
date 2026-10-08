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

std::shared_ptr<pangrova::result::FlightResultServer> FlightServerRegistry::resultServer() {
  std::lock_guard<std::mutex> lock(result_mutex_);
  return result_server_;
}

void FlightServerRegistry::shutdownResultServer() {
  std::shared_ptr<pangrova::result::FlightResultServer> server;
  {
    std::unique_lock<std::mutex> lock(result_mutex_);
    result_cv_.wait(lock, [this] {
      return result_state_ != ResultServiceState::STARTING && result_state_ != ResultServiceState::CLOSING;
    });
    if (!result_server_) return;
    server = result_server_;
    result_state_ = ResultServiceState::CLOSING;
  }
  try {
    server->stop();
    std::lock_guard<std::mutex> lock(result_mutex_);
    result_server_.reset();
    result_state_ = ResultServiceState::STOPPED;
    result_cv_.notify_all();
  } catch (...) {
    std::lock_guard<std::mutex> lock(result_mutex_);
    result_state_ = ResultServiceState::READY;
    result_cv_.notify_all();
    throw;
  }
}

void FlightServerRegistry::shutdownAll() {
  shutdownShuffleServer();
  shutdownResultServer();
}

void FlightServerRegistry::configureShuffleResources(
    const NativeRuntimeConfig& config) {
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
    std::lock_guard<std::mutex> lock(shuffle_mutex_);
    if (shuffle_state_ != ShuffleServiceState::STOPPED || shuffle_server_) {
      throw std::logic_error("Native shuffle limits cannot change while the server exists");
    }
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
  std::lock_guard<std::mutex> lock(shuffle_mutex_);
  if (shuffle_state_ != ShuffleServiceState::STOPPED || shuffle_server_) {
    throw std::logic_error("Native shuffle limits cannot change while the server exists");
  }
  shuffle_limits_ = std::move(limits);
}

void FlightServerRegistry::setHmacKey(std::string currentKey,
                                       std::string previousKey) {
  if (currentKey.size() < 32) {
    throw std::invalid_argument(
        "shuffle HMAC key must contain at least 32 bytes");
  }
  if (!previousKey.empty() && previousKey.size() < 32) {
    throw std::invalid_argument(
        "previous shuffle HMAC key must contain at least 32 bytes");
  }
  std::lock_guard<std::mutex> update(hmac_update_mutex_);
  std::shared_ptr<pangrova::shuffle::FlightShuffleServer> server;
  {
    std::lock_guard<std::mutex> lock(shuffle_mutex_);
    hmac_current_key_ = currentKey;
    hmac_previous_key_ = previousKey;
    ++hmac_revision_;
    server = shuffle_server_;
  }
  if (server && server->isRunning()) server->setHmacKeys(std::move(currentKey), std::move(previousKey));
}

void FlightServerRegistry::configureTransportSecurity(
    bool enabled,
    std::string certificateChainPath,
    std::string privateKeyPath,
    std::string rootCaPath,
    std::string tlsIdentity) {
  requireTransportSecurityInvariant(
      enabled, certificateChainPath, privateKeyPath, rootCaPath, tlsIdentity);
  std::scoped_lock lock(shuffle_mutex_, security_mutex_);
  if (shuffle_state_ != ShuffleServiceState::STOPPED || shuffle_server_) {
    throw std::logic_error("Transport security cannot change while shuffle service exists");
  }
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
