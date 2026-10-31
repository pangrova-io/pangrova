#include "engine/result/ResultPendingRead.h"

#include <algorithm>
#include <arrow/flight/server.h>

namespace pangrova::result {
void ResultPendingRead::complete(ResultPages pages) {
  {
    std::lock_guard<std::mutex> lock(mutex_);
    if (completed_) return;
    pages_ = std::move(pages);
    completed_ = true;
  }
  ready_.notify_all();
}

void ResultPendingRead::cancel(arrow::Status failure) {
  {
    std::lock_guard<std::mutex> lock(mutex_);
    if (completed_) return;
    failure_ = std::move(failure);
    completed_ = true;
  }
  ready_.notify_all();
}

bool ResultPendingRead::isWaiting() const {
  std::lock_guard<std::mutex> lock(mutex_);
  return waiting_ && !completed_;
}

arrow::Result<ResultPages> ResultPendingRead::waitUntil(
    std::chrono::steady_clock::time_point deadline,
    const arrow::flight::ServerCallContext* context) {
  std::unique_lock<std::mutex> lock(mutex_);
  waiting_ = true;
  while (!completed_) {
    if (context && context->is_cancelled()) {
      failure_ = arrow::Status::Cancelled("Result RPC was cancelled");
      completed_ = true;
      break;
    }
    const auto now = std::chrono::steady_clock::now();
    if (now >= deadline) {
      failure_ = arrow::Status::IOError("Result producer timed out before its committed EOF");
      completed_ = true;
      break;
    }
    // Arrow exposes cancellation polling, not an on-cancel subscription. SDK
    // data and task revocation notify this CV immediately; only RPC cancellation
    // needs this bounded poll, independent of the much longer fetch deadline.
    ready_.wait_until(lock, context ? std::min(deadline, now + std::chrono::milliseconds(20)) : deadline);
  }
  if (!failure_.ok()) return failure_;
  return std::move(pages_);
}
}  // namespace pangrova::result
