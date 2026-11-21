#include "command/resource/HandleRegistry.h"

#include <chrono>
#include <stdexcept>

#include "command/task/TaskRequestParser.h"

namespace pangrova::jni {
namespace {
namespace proto = protocol::v2;
constexpr size_t kMaxCreationOwners = 50'000;

int64_t nowEpochMs() {
  return std::chrono::duration_cast<std::chrono::milliseconds>(
      std::chrono::system_clock::now().time_since_epoch()).count();
}
}

void HandleRegistry::purgeClosedCreationsLocked() {
  const auto now = static_cast<uint64_t>(nowEpochMs());
  for (auto current = handles_.begin(); current != handles_.end();) {
    auto retained = current++;
    const auto& entry = retained->second;
    if (!entry.executor && !entry.constructing && entry.identity.has_admission_seal() &&
        entry.identity.admission_seal().deadline_epoch_ms() <= now) eraseLocked(retained);
  }
}

int64_t HandleRegistry::reserveCreationLocked(const proto::ExecutorCreationRecoveryRequest& identity,
                                             bool constructing) {
  purgeClosedCreationsLocked();
  if (handles_.size() >= kMaxCreationOwners) throw std::runtime_error("Native creation ownership is at capacity");
  const auto key = identity.admission_seal().attempt().SerializeAsString();
  if (creation_index_.contains(key)) throw std::runtime_error("Exact Native creation is already owned or sealed");
  const auto handle = next_handle_.fetch_add(1, std::memory_order_relaxed);
  if (handle <= 0) throw std::runtime_error("Native executor handle space exhausted");
  handles_.emplace(handle, Entry{nullptr, current_generation_.load(std::memory_order_relaxed), identity, constructing});
  try {
    creation_index_.emplace(key, handle);
  } catch (...) {
    handles_.erase(handle);
    throw;
  }
  return handle;
}

int64_t HandleRegistry::reserveCreation(const proto::ExecutorCreationRecoveryRequest& identity) {
  validateCreationIdentity(identity);
  if (identity.admission_seal().deadline_epoch_ms() <= static_cast<uint64_t>(nowEpochMs())) {
    throw std::runtime_error("Native creation admission deadline has expired");
  }
  std::unique_lock<std::shared_mutex> lock(mutex_);
  return reserveCreationLocked(identity, true);
}

void HandleRegistry::attachCreation(int64_t handle, std::shared_ptr<executor::TaskExecutor> executor) {
  if (!executor) throw std::invalid_argument("Cannot attach a null Native executor");
  executor->bindHandle(handle, *this);
  std::unique_lock<std::shared_mutex> lock(mutex_);
  auto entry = handles_.find(handle);
  if (entry == handles_.end() || !entry->second.constructing || entry->second.executor) {
    throw std::runtime_error("Native construction owner is not attachable");
  }
  entry->second.executor = std::move(executor);
}

void HandleRegistry::finishCreation(int64_t handle) {
  std::unique_lock<std::shared_mutex> lock(mutex_);
  auto entry = handles_.find(handle);
  if (entry == handles_.end() || !entry->second.constructing) {
    throw std::runtime_error("Native construction exit does not match its reserved owner");
  }
  entry->second.constructing = false;
}

proto::ExecutorCreationRecoveryResult HandleRegistry::recoverCreation(
    const proto::ExecutorCreationRecoveryRequest& identity) {
  validateCreationIdentity(identity);
  proto::ExecutorCreationRecoveryResult result;
  *result.mutable_identity() = identity;
  const auto key = identity.admission_seal().attempt().SerializeAsString();
  std::unique_lock<std::shared_mutex> lock(mutex_);
  auto indexed = creation_index_.find(key);
  if (indexed == creation_index_.end()) {
    // No SDK constructor may run without reserveCreation. Seal the same map
    // before a delayed parse/create call can publish its Native owner.
    reserveCreationLocked(identity, false);
    result.set_state(proto::EXECUTOR_CREATION_STATE_CLOSED_WITHOUT_OWNER);
    return result;
  }
  auto& entry = handles_.at(indexed->second);
  if (entry.identity.SerializeAsString() != identity.SerializeAsString()) {
    throw std::runtime_error("Native recovery identity differs from its exact creation owner");
  }
  if (entry.constructing) result.set_state(proto::EXECUTOR_CREATION_STATE_CONSTRUCTING);
  else if (entry.executor) {
    result.set_state(proto::EXECUTOR_CREATION_STATE_OWNER_PRESENT);
    result.set_executor_handle(static_cast<uint64_t>(indexed->second));
  } else result.set_state(proto::EXECUTOR_CREATION_STATE_CLOSED_WITHOUT_OWNER);
  return result;
}

}  // namespace pangrova::jni
