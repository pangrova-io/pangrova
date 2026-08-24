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

void HandleRegistry::release(int64_t handle) {
  std::unique_lock<std::shared_mutex> lock(mutex_);
  auto it = handles_.find(handle);
  if (it == handles_.end()) {
    throw std::runtime_error(
        "HandleRegistry::release: handle " + std::to_string(handle) +
        " not found (double release or stale handle)");
  }
  handles_.erase(it);
}

void HandleRegistry::destroyAll() {
  std::unique_lock<std::shared_mutex> lock(mutex_);
  for (auto& [handle, entry] : handles_) {
    auto state = entry.executor ? entry.executor->state()
                                : pangrova::executor::ExecutorState::FINISHED;
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
  }
  handles_.clear();
  current_generation_.fetch_add(1, std::memory_order_release);
}

void HandleRegistry::removeTerminated() {
  // Two-phase lock: shared lock to collect, exclusive lock to remove.
  // TOCTOU between phases is benign: handle IDs are monotonically increasing
  // (next_handle_ never reuses), so a handle collected in Phase 1 that gets
  // released by another thread before Phase 2 simply results in a no-op erase.
  // Phase 1: Read lock to collect terminated handle IDs.
  std::vector<int64_t> terminated;
  {
    std::shared_lock<std::shared_mutex> lock(mutex_);
    for (const auto& [handle, entry] : handles_) {
      if (entry.executor) {
        auto s = entry.executor->state();
        if (s == pangrova::executor::ExecutorState::FINISHED ||
            s == pangrova::executor::ExecutorState::FAILED ||
            s == pangrova::executor::ExecutorState::CANCELLED) {
          terminated.push_back(handle);
        }
      }
    }
  }

  // Phase 2: Write lock only for removal of collected handles.
  if (!terminated.empty()) {
    std::unique_lock<std::shared_mutex> lock(mutex_);
    for (auto handle : terminated) {
      handles_.erase(handle);
    }
  }
}

size_t HandleRegistry::size() const {
  std::shared_lock<std::shared_mutex> lock(mutex_);
  return handles_.size();
}

uint64_t HandleRegistry::generation() const {
  return current_generation_.load(std::memory_order_acquire);
}

}  // namespace pangrova::jni
