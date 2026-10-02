// Copyright (c) Pangrova contributors. All rights reserved.
// Licensed under the Apache License, Version 2.0.

#include "jni/registry/HandleRegistry.h"

#include <iostream>
#include <stdexcept>
#include <vector>

namespace pangrova::jni {

HandleRegistry& HandleRegistry::instance() {
  static HandleRegistry inst;
  return inst;
}

int64_t HandleRegistry::create(
    std::shared_ptr<pangrova::executor::TaskExecutor> executor) {
  if (!executor) {
    throw std::runtime_error("HandleRegistry::create: null executor");
  }
  int64_t handle = next_handle_.fetch_add(1, std::memory_order_relaxed);
  executor->bindHandle(handle);
  {
    // F-NATIVE-04: Read current_generation_ inside the lock to prevent TOCTOU
    // race with destroyAll() which increments generation under exclusive lock.
    std::unique_lock<std::shared_mutex> lock(mutex_);
    uint64_t gen = current_generation_.load(std::memory_order_relaxed);
    handles_[handle] = Entry{std::move(executor), gen};
  }
  return handle;
}

std::shared_ptr<pangrova::executor::TaskExecutor> HandleRegistry::acquire(
    int64_t handle) const {
  std::shared_lock<std::shared_mutex> lock(mutex_);
  auto it = handles_.find(handle);
  if (it == handles_.end()) {
    throw std::runtime_error(
        "HandleRegistry::acquire: handle " + std::to_string(handle) +
        " not found (already released or never created)");
  }
  uint64_t gen = current_generation_.load(std::memory_order_acquire);
  if (it->second.generation != gen) {
    throw std::runtime_error(
        "HandleRegistry::acquire: handle " + std::to_string(handle) +
        " is stale (generation " + std::to_string(it->second.generation) +
        " vs current " + std::to_string(gen) + ")");
  }
  return it->second.executor;
}

void HandleRegistry::release(
    int64_t handle,
    const std::function<void(pangrova::executor::TaskExecutor&)>& prepare_receipt) {
  // Quiesce actual readers, buffers and drivers before removing ownership.
  // A failed resource drain leaves the exact handle available for retry.
  auto executor = acquire(handle);
  executor->release();
  if (prepare_receipt) prepare_receipt(*executor);
  std::unique_lock<std::shared_mutex> lock(mutex_);
  auto it = handles_.find(handle);
  if (it == handles_.end() || it->second.executor != executor) {
    throw std::runtime_error(
        "HandleRegistry::release: handle " + std::to_string(handle) +
        " not found (double release or stale handle)");
  }
  handles_.erase(it);
}

void HandleRegistry::destroyAll() {
  std::vector<std::pair<int64_t, Entry>> snapshot;
  {
    std::shared_lock<std::shared_mutex> lock(mutex_);
    for (const auto& entry : handles_) snapshot.push_back(entry);
  }
  for (const auto& [handle, entry] : snapshot) {
    auto state = entry.executor->state();
    bool terminal =
        (state == pangrova::executor::ExecutorState::FINISHED ||
         state == pangrova::executor::ExecutorState::FAILED ||
         state == pangrova::executor::ExecutorState::CANCELLED);
    if (!terminal) {
      std::cerr << "level=WARN event=handle_leaked_on_shutdown"
                << " handle=" << handle
                << " taskId="
                << (entry.executor ? entry.executor->taskId() : "null")
                << std::endl;
    }
    entry.executor->release();
    std::unique_lock<std::shared_mutex> lock(mutex_);
    auto current = handles_.find(handle);
    if (current != handles_.end() && current->second.executor == entry.executor) handles_.erase(current);
  }
  std::unique_lock<std::shared_mutex> lock(mutex_);
  if (!handles_.empty()) throw std::runtime_error("Executors were registered during shutdown");
  current_generation_.fetch_add(1, std::memory_order_release);
}

size_t HandleRegistry::size() const {
  std::shared_lock<std::shared_mutex> lock(mutex_);
  return handles_.size();
}

uint64_t HandleRegistry::generation() const {
  return current_generation_.load(std::memory_order_acquire);
}

}  // namespace pangrova::jni
