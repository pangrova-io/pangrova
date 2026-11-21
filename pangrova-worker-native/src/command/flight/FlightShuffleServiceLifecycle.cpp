#include "command/flight/FlightServerRegistry.h"

#include <stdexcept>
#include <exception>

#include "engine/shuffle/FlightShuffleServer.h"

namespace pangrova::jni {

void FlightServerRegistry::activateShuffleRuntime() {
  std::lock_guard<std::mutex> lock(shuffle_mutex_);
  if (shuffle_state_ != ShuffleServiceState::STOPPED || shuffle_server_) {
    throw std::logic_error("Cannot activate shuffle runtime while its service exists");
  }
  shuffle_runtime_sealed_ = false;
}

std::shared_ptr<pangrova::shuffle::FlightShuffleServer>
FlightServerRegistry::acquireShuffleServer() {
  pangrova::shuffle::FlightShuffleServerConfig config;
  {
    std::unique_lock<std::mutex> lock(shuffle_mutex_);
    shuffle_cv_.wait(lock, [this] { return shuffle_state_ != ShuffleServiceState::STARTING; });
    if (shuffle_runtime_sealed_) throw std::logic_error("shuffle runtime is sealed");
    if (shuffle_state_ == ShuffleServiceState::READY) {
      if (!shuffle_server_->isRunning()) throw std::runtime_error("Native shuffle service exited");
      return shuffle_server_;
    }
    if (shuffle_state_ != ShuffleServiceState::STOPPED) {
      throw std::logic_error("shuffle service is closing");
    }
    if (hmac_current_key_.empty()) {
      throw std::runtime_error("Refusing to start Native shuffle without an HMAC ticket key");
    }
    if (!shuffle_limits_) {
      throw std::runtime_error("Refusing to start Native shuffle without a prebooked service budget");
    }
    config.buffer_limits = *shuffle_limits_;
    config.hmac_current_key = hmac_current_key_;
    config.hmac_previous_key = hmac_previous_key_;
    shuffle_state_ = ShuffleServiceState::STARTING;
  }
  return startShuffleService(std::move(config));
}

std::shared_ptr<pangrova::shuffle::FlightShuffleServer>
FlightServerRegistry::startShuffleService(pangrova::shuffle::FlightShuffleServerConfig config) {
  std::shared_ptr<pangrova::shuffle::FlightShuffleServer> candidate;
  try {
    {
      std::lock_guard<std::mutex> lock(security_mutex_);
      if (transport_tls_enabled_) {
        config.tls_cert_chain_path = transport_tls_certificate_chain_path_;
        config.tls_private_key_path = transport_tls_private_key_path_;
        config.tls_root_ca_path = transport_tls_root_ca_path_;
      }
    }
    candidate = std::make_shared<pangrova::shuffle::FlightShuffleServer>(std::move(config));
    candidate->start();
    // Configuration may rotate while network initialization runs outside the
    // registry lock. Publish only a verifier/signing snapshot from that revision.
    for (;;) {
      std::string current_key, previous_key;
      uint64_t revision;
      {
        std::lock_guard<std::mutex> lock(shuffle_mutex_);
        current_key = hmac_current_key_;
        previous_key = hmac_previous_key_;
        revision = hmac_revision_;
      }
      candidate->setHmacKeys(std::move(current_key), std::move(previous_key));
      std::lock_guard<std::mutex> lock(shuffle_mutex_);
      if (revision != hmac_revision_) continue;
      shuffle_server_ = candidate;
      shuffle_state_ = ShuffleServiceState::READY;
      shuffle_cv_.notify_all();
      break;
    }
  } catch (...) {
    // Close a partially started service before allowing a replacement owner.
    const auto startup_error = std::current_exception();
    try {
      if (candidate) candidate->stop();
    } catch (...) {
      std::lock_guard<std::mutex> lock(shuffle_mutex_);
      shuffle_server_ = candidate;
      shuffle_state_ = ShuffleServiceState::READY;
      shuffle_runtime_sealed_ = true;
      shuffle_cv_.notify_all();
      throw;
    }
    std::lock_guard<std::mutex> lock(shuffle_mutex_);
    shuffle_state_ = ShuffleServiceState::STOPPED;
    shuffle_cv_.notify_all();
    std::rethrow_exception(startup_error);
  }
  {
    std::lock_guard<std::mutex> lock(shuffle_mutex_);
    if (shuffle_runtime_sealed_) throw std::logic_error("shuffle runtime closed during startup");
  }
  return candidate;
}

int FlightServerRegistry::ensureShuffleServerRunning() {
  const auto server = acquireShuffleServer();
  auto control = server->acquireControl();
  return server->boundPort();
}

ShuffleRegistrationLease FlightServerRegistry::registerShuffleHandle(
    int64_t handle, pangrova::shuffle::SerializedShuffleQueue* queue,
    const std::string& query_id, int32_t stage_id, int32_t partition_count,
    uint64_t plan_epoch, uint32_t attempt_id, uint64_t fencing_token, bool disk_only) {
  auto server = acquireShuffleServer();
  auto control = server->acquireControl();
  auto registration = server->registerHandle(
      handle, queue, query_id, stage_id, partition_count, plan_epoch, attempt_id, fencing_token, disk_only);
  const int port = server->boundPort();
  return {std::move(server), std::move(control), std::move(registration), port};
}

void FlightServerRegistry::unregisterShuffleHandle(int64_t handle) {
  std::shared_ptr<pangrova::shuffle::FlightShuffleServer> server;
  {
    std::lock_guard<std::mutex> lock(shuffle_mutex_);
    server = shuffle_server_;
  }
  if (server) server->unregisterHandle(handle);
}

void FlightServerRegistry::shutdownShuffleServer() {
  std::shared_ptr<pangrova::shuffle::FlightShuffleServer> server;
  {
    std::unique_lock<std::mutex> lock(shuffle_mutex_);
    shuffle_runtime_sealed_ = true;
    shuffle_cv_.wait(lock, [this] {
      return shuffle_state_ != ShuffleServiceState::STARTING &&
          shuffle_state_ != ShuffleServiceState::CLOSING;
    });
    if (!shuffle_server_) return;
    server = shuffle_server_;
    shuffle_state_ = ShuffleServiceState::CLOSING;
  }
  try {
    server->stop();
    std::lock_guard<std::mutex> lock(shuffle_mutex_);
    shuffle_server_.reset();  // The local owner defers destruction beyond unlock.
    shuffle_state_ = ShuffleServiceState::STOPPED;
    shuffle_cv_.notify_all();
  } catch (...) {
    std::lock_guard<std::mutex> lock(shuffle_mutex_);
    shuffle_state_ = ShuffleServiceState::READY;
    shuffle_cv_.notify_all();
    throw;
  }
}

}  // namespace pangrova::jni
