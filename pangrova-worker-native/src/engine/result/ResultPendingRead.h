#pragma once

#include <arrow/result.h>
#include <chrono>
#include <cstdint>
#include <condition_variable>
#include <memory>
#include <mutex>
#include <vector>
#include <folly/io/IOBuf.h>

namespace arrow::flight { class ServerCallContext; }

namespace pangrova::result {
struct ResultPages {
  std::vector<std::unique_ptr<folly::IOBuf>> pages;
  int64_t sequence;
};

// One actual SDK getData callback competes with revocation/cancellation. Late
// callbacks relinquish their pages without completing a second time.
class ResultPendingRead {
 public:
  void complete(ResultPages pages);
  void cancel(arrow::Status failure);
  bool isWaiting() const;
  arrow::Result<ResultPages> waitUntil(std::chrono::steady_clock::time_point deadline,
      const arrow::flight::ServerCallContext* context);

 private:
  mutable std::mutex mutex_;
  std::condition_variable ready_;
  bool completed_ = false;
  bool waiting_ = false;
  arrow::Status failure_ = arrow::Status::OK();
  ResultPages pages_{{}, 0};
};
}  // namespace pangrova::result
