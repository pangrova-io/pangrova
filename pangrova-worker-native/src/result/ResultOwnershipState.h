#pragma once

#include <cstdint>
#include <memory>
#include <mutex>
#include <arrow/record_batch.h>

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
  void revokeAndClearQueue();
  void acquireReader();
  void releaseReader() noexcept;
  void acquireBatch();
  void releaseBatch() noexcept;
  Evidence evidence() const;

 private:
  mutable std::mutex mutex_;
  Evidence evidence_{false, false, false, 0, 0};
};

// The root lease covers zero-column batches; recursive buffer leases cover
// consumers retaining a column/slice after releasing its RecordBatch root.
std::shared_ptr<arrow::RecordBatch> retainResultBatch(
    const std::shared_ptr<arrow::RecordBatch>& batch,
    const std::shared_ptr<ResultOwnershipState>& state);
}  // namespace pangrova::result
