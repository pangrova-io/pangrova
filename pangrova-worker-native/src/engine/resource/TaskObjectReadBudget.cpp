#include "engine/resource/TaskObjectReadBudget.h"

#include <stdexcept>

#include "velox/common/file/File.h"

namespace pangrova::resource {

TaskObjectReadBudget::TaskObjectReadBudget(uint64_t limit) : limit_(limit) {}
TaskObjectReadBudget::~TaskObjectReadBudget() = default;

facebook::velox::IoStats* TaskObjectReadBudget::s3IoStats() {
  std::lock_guard<std::mutex> lock(mutex_);
  if (!s3_io_stats_) s3_io_stats_ = std::make_unique<facebook::velox::IoStats>();
  return s3_io_stats_.get();
}

TaskObjectReadBudget::Attempt TaskObjectReadBudget::begin(uint64_t bytes, Operation operation) {
  auto owner = shared_from_this();
  std::lock_guard<std::mutex> lock(mutex_);
  if (stopped_) throw std::runtime_error("Task object read cancelled or released");
  if (bytes > limit_ - counters_.requested_bytes) {
    throw std::runtime_error("Task object-store requested-read grant exhausted");
  }
  counters_.requested_bytes += bytes;
  ++active_;
  return Attempt(std::move(owner), bytes, operation);
}

TaskObjectReadBudget::Attempt::Attempt(
    std::shared_ptr<TaskObjectReadBudget> owner, uint64_t bytes, Operation operation) noexcept
    : owner_(std::move(owner)), reserved_bytes_(bytes), operation_(operation) {}

TaskObjectReadBudget::Attempt::~Attempt() {
  if (!owner_) return;
  std::lock_guard<std::mutex> lock(owner_->mutex_);
  auto& counts = owner_->counters_;
  if (operation_ == Operation::METADATA) {
    counts.metadata_calls += calls_;
  } else {
    counts.payload_calls += calls_;
  }
  counts.failed_calls += calls_ - completed_calls_;
  counts.returned_bytes += returned_bytes_;
  --owner_->active_;
  if (owner_->active_ == 0) owner_->drained_.notify_all();
}

void TaskObjectReadBudget::Attempt::completeCall(uint64_t bytes) {
  if (completed_calls_ == calls_ || bytes > reserved_bytes_ - returned_bytes_) {
    throw std::runtime_error("Task object read completion exceeds its admitted range or call count");
  }
  returned_bytes_ += bytes;
  ++completed_calls_;
}

void TaskObjectReadBudget::requestCancel() {
  std::lock_guard<std::mutex> lock(mutex_);
  stopped_ = true;
}

void TaskObjectReadBudget::stopAndDrain() {
  std::unique_lock<std::mutex> lock(mutex_);
  stopped_ = true;
  drained_.wait(lock, [this] { return active_ == 0; });
}

TaskObjectReadBudget::Counters TaskObjectReadBudget::snapshot() const {
  std::lock_guard<std::mutex> lock(mutex_);
  auto result = counters_;
  if (s3_io_stats_) {
    const auto stats = s3_io_stats_->stats();
    const auto sum = [&stats](const char* key) {
      const auto item = stats.find(key);
      return item == stats.end() ? 0 : item->second.sum;
    };
    result.s3_sdk_attempts = sum("s3SdkReadAttempts");
    result.s3_sdk_retries = sum("s3SdkReadRetries");
    result.s3_response_body_bytes = sum("s3SdkResponseBodyBytes");
  }
  return result;
}
uint64_t TaskObjectReadBudget::activeReads() const {
  std::lock_guard<std::mutex> lock(mutex_);
  return active_;
}

}  // namespace pangrova::resource
