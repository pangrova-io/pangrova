#pragma once

#include <arrow/flight/types.h>
#include <arrow/ipc/writer.h>

#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <functional>
#include <list>
#include <memory>
#include <mutex>
#include <vector>

namespace pangrova::result {

class ResultOwnershipState;
namespace detail { struct ResultDelivery; }

// Ready and in-flight IPC buffers share one window. A blocked driver owns one
// unqueued batch of frames until it obtains credit or cancellation releases it.
class ResultPayloadQueue final : public std::enable_shared_from_this<ResultPayloadQueue> {
 public:
  class Frame {
   public:
    Frame() = default;
    Frame(arrow::ipc::IpcPayload payload, int64_t bytes,
        std::shared_ptr<arrow::MemoryPool> memory, std::shared_ptr<ResultOwnershipState> ownership,
        std::shared_ptr<void> source = {});
    Frame(Frame&&) noexcept = default;
    Frame& operator=(Frame&&) noexcept = default;
    explicit operator bool() const { return owner_ != nullptr; }
    arrow::flight::FlightPayload flightPayload() const;

   private:
    friend class ResultPayloadQueue;
    std::shared_ptr<detail::ResultDelivery> owner_;
  };
  using WakeProducer = std::function<void(const arrow::Status&)>;

  explicit ResultPayloadQueue(int64_t windowBytes);

  arrow::Result<bool> tryPush(Frame& frame);
  arrow::Status pushOrDefer(std::vector<Frame> frames, WakeProducer wake);
  arrow::Result<Frame> next(std::chrono::steady_clock::time_point deadline,
      const std::function<bool()>& cancelled);
  arrow::Status seal();
  void abort(const arrow::Status& failure);
  int64_t retainedBytes() const;

 private:
  friend struct detail::ResultDelivery;

  struct BlockedProducer {
    Frame frame;
    WakeProducer wake;
  };

  bool hasCredit(int64_t bytes) const;
  void admit(Frame& frame);
  void publish(Frame& frame);
  void release(int64_t bytes);
  void promote();

  const int64_t window_bytes_;
  mutable std::mutex mutex_;
  std::condition_variable available_;
  std::list<BlockedProducer> ready_;
  std::list<BlockedProducer> waiting_;
  int64_t retained_bytes_ = 0;
  bool sealed_ = false;
  arrow::Status failure_;
};

}  // namespace pangrova::result
