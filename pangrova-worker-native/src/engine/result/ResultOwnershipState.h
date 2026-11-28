#pragma once

#include <atomic>
#include <cstdint>
#include <functional>
#include <mutex>

namespace pangrova::result {
// Tracks actual producer, RPC reader and exported buffer lifetimes independently.
class ResultOwnershipState {
 public:
  struct Evidence {
    bool producer_stopped;
    bool authority_revoked;
    bool queue_cleared;
    uint64_t active_readers;
    uint64_t retained_batch_owners;
    bool drained() const;
  };
  void stopProducer();
  // Returns true for the caller that first revokes this authority.
  bool revokeAuthority();
  // Flight checks admission on every payload without taking the lifetime mutex.
  bool authorityRevoked() const noexcept {
    return authority_revoked_.load(std::memory_order_acquire);
  }
  void revokeAndClearQueue();
  void acquireReader();
  void releaseReader() noexcept;
  bool tryAcquireBatch();
  void acquireBatch();
  void releaseBatch() noexcept;
  Evidence evidence() const;
  // Single subscription. An already drained owner delivers immediately, outside the mutex.
  void watchDrain(std::function<void()> listener);

 private:
  Evidence evidenceLocked() const;
  std::function<void()> takeDrainedListenerLocked();
  mutable std::mutex mutex_;
  std::atomic<bool> authority_revoked_{false};
  bool producer_stopped_ = false;
  bool queue_cleared_ = false;
  uint64_t active_readers_ = 0;
  uint64_t retained_batch_owners_ = 0;
  std::function<void()> drained_listener_;
  bool drain_watched_ = false;
};

}  // namespace pangrova::result
