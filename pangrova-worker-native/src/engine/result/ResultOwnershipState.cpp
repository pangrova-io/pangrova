#include "engine/result/ResultOwnershipState.h"

#include <stdexcept>
#include <utility>

namespace pangrova::result {
bool ResultOwnershipState::Evidence::drained() const {
  return producer_stopped && authority_revoked && queue_cleared &&
      active_readers == 0 && retained_batch_owners == 0;
}
void ResultOwnershipState::stopProducer() {
  std::function<void()> notify;
  {
    std::lock_guard<std::mutex> lock(mutex_);
    evidence_.producer_stopped = true;
    notify = takeDrainedListenerLocked();
  }
  if (notify) notify();
}
bool ResultOwnershipState::revokeAuthority() {
  std::lock_guard<std::mutex> lock(mutex_);
  return !std::exchange(evidence_.authority_revoked, true);
}
void ResultOwnershipState::revokeAndClearQueue() {
  revokeAuthority();
  std::function<void()> notify;
  {
    std::lock_guard<std::mutex> lock(mutex_);
    evidence_.queue_cleared = true;
    notify = takeDrainedListenerLocked();
  }
  if (notify) notify();
}
void ResultOwnershipState::acquireReader() {
  std::lock_guard<std::mutex> lock(mutex_);
  if (evidence_.authority_revoked) throw std::runtime_error("Result authority was revoked");
  // One authorized Flight stream owns the attempt.
  if (evidence_.active_readers != 0) throw std::runtime_error("Result stream already has a reader");
  ++evidence_.active_readers;
}
void ResultOwnershipState::releaseReader() noexcept {
  std::function<void()> notify;
  {
    std::lock_guard<std::mutex> lock(mutex_);
    if (evidence_.active_readers == 0) std::terminate();
    --evidence_.active_readers;
    notify = takeDrainedListenerLocked();
  }
  if (notify) notify();
}
bool ResultOwnershipState::tryAcquireBatch() {
  std::lock_guard<std::mutex> lock(mutex_);
  if (evidence_.authority_revoked) return false;
  ++evidence_.retained_batch_owners;
  return true;
}
void ResultOwnershipState::acquireBatch() {
  std::lock_guard<std::mutex> lock(mutex_);
  ++evidence_.retained_batch_owners;
}
void ResultOwnershipState::releaseBatch() noexcept {
  std::function<void()> notify;
  {
    std::lock_guard<std::mutex> lock(mutex_);
    if (evidence_.retained_batch_owners == 0) std::terminate();
    --evidence_.retained_batch_owners;
    notify = takeDrainedListenerLocked();
  }
  if (notify) notify();
}
ResultOwnershipState::Evidence ResultOwnershipState::evidence() const {
  std::lock_guard<std::mutex> lock(mutex_);
  return evidence_;
}

void ResultOwnershipState::watchDrain(std::function<void()> listener) {
  if (!listener) throw std::invalid_argument("Result drain listener is required");
  std::function<void()> notify;
  {
    std::lock_guard<std::mutex> lock(mutex_);
    if (drain_watched_) throw std::logic_error("Result drain already has a listener");
    drain_watched_ = true;
    drained_listener_ = std::move(listener);
    notify = takeDrainedListenerLocked();
  }
  if (notify) notify();
}

std::function<void()> ResultOwnershipState::takeDrainedListenerLocked() {
  if (drained_listener_ && evidence_.drained()) return std::exchange(drained_listener_, {});
  return {};
}

}  // namespace pangrova::result
