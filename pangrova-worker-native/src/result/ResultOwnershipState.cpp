#include "result/ResultOwnershipState.h"

#include <arrow/array.h>
#include <stdexcept>
#include "result/ResultPendingRead.h"

namespace pangrova::result {
bool ResultOwnershipState::Evidence::drained() const {
  return producer_stopped && authority_revoked && queue_cleared &&
      active_readers == 0 && retained_batch_owners == 0;
}
void ResultOwnershipState::stopProducer() {
  std::lock_guard<std::mutex> lock(mutex_);
  evidence_.producer_stopped = true;
}
void ResultOwnershipState::revokeAuthority() {
  std::shared_ptr<ResultPendingRead> pending;
  {
    std::lock_guard<std::mutex> lock(mutex_);
    evidence_.authority_revoked = true;
    pending = pending_read_.lock();
  }
  if (pending) pending->cancel(arrow::Status::Cancelled("Result authority was revoked"));
}
void ResultOwnershipState::revokeAndClearQueue() {
  revokeAuthority();
  std::lock_guard<std::mutex> lock(mutex_);
  evidence_.queue_cleared = true;
}
arrow::Status ResultOwnershipState::attachPendingRead(const std::shared_ptr<ResultPendingRead>& pending) {
  bool revoked;
  {
    std::lock_guard<std::mutex> lock(mutex_);
    if (!pending_read_.expired()) return arrow::Status::Invalid("Result reader already has an outstanding fetch");
    pending_read_ = pending;
    revoked = evidence_.authority_revoked;
  }
  if (revoked) pending->cancel(arrow::Status::Cancelled("Result authority was revoked"));
  return arrow::Status::OK();
}
void ResultOwnershipState::detachPendingRead(const std::shared_ptr<ResultPendingRead>& pending) {
  std::lock_guard<std::mutex> lock(mutex_);
  if (pending_read_.lock() == pending) pending_read_.reset();
}
bool ResultOwnershipState::hasPendingRead() const {
  std::shared_ptr<ResultPendingRead> pending;
  {
    std::lock_guard<std::mutex> lock(mutex_);
    pending = pending_read_.lock();
  }
  return pending && pending->isWaiting();
}
void ResultOwnershipState::acquireReader() {
  std::lock_guard<std::mutex> lock(mutex_);
  if (evidence_.authority_revoked) throw std::runtime_error("Result authority was revoked");
  // Destination zero has one canonical sequence/ACK owner. Service admission
  // already enforces this through TaskOutputInfo::claimed; retain the same
  // invariant for direct reader construction, without replacing its callback.
  if (evidence_.active_readers != 0) throw std::runtime_error("Result stream already has a reader");
  ++evidence_.active_readers;
}
void ResultOwnershipState::releaseReader() noexcept {
  std::lock_guard<std::mutex> lock(mutex_);
  if (evidence_.active_readers == 0) std::terminate();
  --evidence_.active_readers;
}
void ResultOwnershipState::acquireBatch() {
  std::lock_guard<std::mutex> lock(mutex_);
  ++evidence_.retained_batch_owners;
}
void ResultOwnershipState::releaseBatch() noexcept {
  std::lock_guard<std::mutex> lock(mutex_);
  if (evidence_.retained_batch_owners == 0) std::terminate();
  --evidence_.retained_batch_owners;
}
ResultOwnershipState::Evidence ResultOwnershipState::evidence() const {
  std::lock_guard<std::mutex> lock(mutex_);
  return evidence_;
}

namespace {
struct BatchLease {
  std::shared_ptr<ResultOwnershipState> state;
  explicit BatchLease(std::shared_ptr<ResultOwnershipState> value) : state(std::move(value)) {
    state->acquireBatch();
  }
  ~BatchLease() { state->releaseBatch(); }
};
struct BufferOwner {
  std::shared_ptr<BatchLease> lease;
  std::shared_ptr<arrow::Buffer> buffer;
};
std::shared_ptr<arrow::ArrayData> retainData(const std::shared_ptr<arrow::ArrayData>& data,
    const std::shared_ptr<BatchLease>& lease) {
  auto result = data->Copy();
  for (auto& buffer : result->buffers) {
    if (!buffer) continue;
    auto owner = std::make_shared<BufferOwner>(BufferOwner{lease, buffer});
    buffer = std::shared_ptr<arrow::Buffer>(owner, owner->buffer.get());
  }
  for (auto& child : result->child_data) child = retainData(child, lease);
  if (result->dictionary) result->dictionary = retainData(result->dictionary, lease);
  return result;
}
}  // namespace

std::shared_ptr<arrow::RecordBatch> retainResultBatch(
    const std::shared_ptr<arrow::RecordBatch>& batch,
    const std::shared_ptr<ResultOwnershipState>& state) {
  if (!batch || !state) throw std::invalid_argument("Result batch lacks its lifetime authority");
  auto lease = std::make_shared<BatchLease>(state);
  std::vector<std::shared_ptr<arrow::Array>> columns;
  columns.reserve(batch->num_columns());
  for (const auto& column : batch->columns()) {
    columns.push_back(arrow::MakeArray(retainData(column->data(), lease)));
  }
  struct RootOwner {
    std::shared_ptr<BatchLease> lease;
    std::shared_ptr<arrow::RecordBatch> batch;
  };
  auto owner = std::make_shared<RootOwner>(RootOwner{std::move(lease),
      arrow::RecordBatch::Make(batch->schema(), batch->num_rows(), std::move(columns))});
  return std::shared_ptr<arrow::RecordBatch>(owner, owner->batch.get());
}
}  // namespace pangrova::result
