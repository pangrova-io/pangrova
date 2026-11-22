#pragma once

#include <cstdint>
#include <functional>
#include <memory>
#include <mutex>
#include <arrow/record_batch.h>

namespace pangrova::result {
class ResultPendingRead;
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
  arrow::Status attachPendingRead(const std::shared_ptr<ResultPendingRead>& pending);
  void detachPendingRead(const std::shared_ptr<ResultPendingRead>& pending);
  bool hasPendingRead() const;
  void acquireReader();
  void releaseReader() noexcept;
  void acquireBatch();
  void releaseBatch() noexcept;
  Evidence evidence() const;
  // Single subscription. An already drained owner delivers immediately, outside the mutex.
  void watchDrain(std::function<void()> listener);

 private:
  std::function<void()> takeDrainedListenerLocked();
  mutable std::mutex mutex_;
  Evidence evidence_{false, false, false, 0, 0};
  std::weak_ptr<ResultPendingRead> pending_read_;
  std::function<void()> drained_listener_;
  bool drain_watched_ = false;
};

// The root lease covers zero-column batches; recursive buffer leases cover
// consumers retaining a column/slice after releasing its RecordBatch root.
std::shared_ptr<arrow::RecordBatch> retainResultBatch(
    const std::shared_ptr<arrow::RecordBatch>& batch,
    const std::shared_ptr<ResultOwnershipState>& state);
}  // namespace pangrova::result
