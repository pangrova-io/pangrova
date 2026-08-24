// Copyright (c) Pangrova contributors. All rights reserved.
// Licensed under the Apache License, Version 2.0.

// FlightServerRegistry: Thread-safe singleton managing FlightResultServer
// and the shared multiplexed FlightShuffleServer instance.

#include "jni/command/FlightServerRegistry.h"

#include "result/FlightResultServer.h"
#include "shuffle/FlightShuffleServer.h"

namespace pangrova::jni {

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
  // Create and start the shared shuffle server with HMAC key config.
  pangrova::shuffle::FlightShuffleServerConfig config;
  config.reuse_port = true;
  config.hmac_current_key = hmac_current_key_;
  config.hmac_previous_key = hmac_previous_key_;
  shuffle_server_ = std::make_unique<pangrova::shuffle::FlightShuffleServer>(config);
  return shuffle_server_->start();
}

void FlightServerRegistry::registerShuffleHandle(
    int64_t handle,
    pangrova::shuffle::SerializedShuffleQueue* queue) {
  std::lock_guard<std::mutex> lock(shuffle_mutex_);
  if (!shuffle_server_ || !shuffle_server_->isRunning()) {
    // Auto-start the shared server if not yet running.
    pangrova::shuffle::FlightShuffleServerConfig config;
    config.reuse_port = true;
    config.hmac_current_key = hmac_current_key_;
    config.hmac_previous_key = hmac_previous_key_;
    shuffle_server_ = std::make_unique<pangrova::shuffle::FlightShuffleServer>(config);
    shuffle_server_->start();
  }
  shuffle_server_->registerHandle(handle, queue);
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

void FlightServerRegistry::setHmacKey(std::string currentKey,
                                       std::string previousKey) {
  std::lock_guard<std::mutex> lock(shuffle_mutex_);
  hmac_current_key_ = std::move(currentKey);
  hmac_previous_key_ = std::move(previousKey);
}

}  // namespace pangrova::jni
