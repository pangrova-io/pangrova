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
    producer_stopped_ = true;
    notify = takeDrainedListenerLocked();
  }
  if (notify) notify();
}
bool ResultOwnershipState::revokeAuthority() {
  std::lock_guard<std::mutex> lock(mutex_);
  return !authority_revoked_.exchange(true, std::memory_order_acq_rel);
}
void ResultOwnershipState::revokeAndClearQueue() {
  std::function<void()> notify;
  {
    std::lock_guard<std::mutex> lock(mutex_);
    authority_revoked_.store(true, std::memory_order_release);
    queue_cleared_ = true;
    notify = takeDrainedListenerLocked();
  }
  if (notify) notify();
}
void ResultOwnershipState::acquireReader() {
  std::lock_guard<std::mutex> lock(mutex_);
  if (authorityRevoked()) throw std::runtime_error("Result authority was revoked");
  // One authorized Flight stream owns the attempt.
  if (active_readers_ != 0) throw std::runtime_error("Result stream already has a reader");
  ++active_readers_;
}
void ResultOwnershipState::releaseReader() noexcept {
  std::function<void()> notify;
  {
    std::lock_guard<std::mutex> lock(mutex_);
    if (active_readers_ == 0) std::terminate();
    --active_readers_;
    notify = takeDrainedListenerLocked();
  }
  if (notify) notify();
}
bool ResultOwnershipState::tryAcquireBatch() {
  std::lock_guard<std::mutex> lock(mutex_);
  if (authorityRevoked()) return false;
  ++retained_batch_owners_;
  return true;
}
void ResultOwnershipState::acquireBatch() {
  std::lock_guard<std::mutex> lock(mutex_);
  ++retained_batch_owners_;
}
void ResultOwnershipState::releaseBatch() noexcept {
  std::function<void()> notify;
  {
    std::lock_guard<std::mutex> lock(mutex_);
    if (retained_batch_owners_ == 0) std::terminate();
    --retained_batch_owners_;
    notify = takeDrainedListenerLocked();
  }
  if (notify) notify();
}
ResultOwnershipState::Evidence ResultOwnershipState::evidence() const {
  std::lock_guard<std::mutex> lock(mutex_);
  return evidenceLocked();
}

ResultOwnershipState::Evidence ResultOwnershipState::evidenceLocked() const {
  return {producer_stopped_, authorityRevoked(), queue_cleared_,
      active_readers_, retained_batch_owners_};
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
  if (drained_listener_ && evidenceLocked().drained()) return std::exchange(drained_listener_, {});
  return {};
}

}  // namespace pangrova::result
