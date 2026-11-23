#pragma once

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
  std::function<void()> takeDrainedListenerLocked();
  mutable std::mutex mutex_;
  Evidence evidence_{false, false, false, 0, 0};
  std::function<void()> drained_listener_;
  bool drain_watched_ = false;
};

}  // namespace pangrova::result
