#include "engine/result/ResultPayloadQueue.h"

#include "engine/result/ResultOwnershipState.h"

#include <algorithm>
#include <stdexcept>
#include <utility>

namespace pangrova::result {
namespace detail {

struct ResultDelivery {
  ResultDelivery(arrow::ipc::IpcPayload payload, int64_t bytes,
      std::shared_ptr<arrow::MemoryPool> memory, std::shared_ptr<ResultOwnershipState> ownership,
      std::shared_ptr<void> source)
      : ownership(std::move(ownership)), memory(std::move(memory)), source(std::move(source)), payload(std::move(payload)), bytes(bytes) {
    this->ownership->acquireBatch();
  }

  ~ResultDelivery() {
    payload = arrow::ipc::IpcPayload{};
    memory.reset();
    source.reset();
    ownership->releaseBatch();
    if (const auto destination = queue.lock()) destination->release(bytes);
  }

  std::weak_ptr<ResultPayloadQueue> queue;
  std::shared_ptr<ResultOwnershipState> ownership;
  std::shared_ptr<arrow::MemoryPool> memory;
  std::shared_ptr<void> source;
  arrow::ipc::IpcPayload payload;
  int64_t bytes;
};

}  // namespace detail

ResultPayloadQueue::Frame::Frame(arrow::ipc::IpcPayload payload, int64_t bytes,
    std::shared_ptr<arrow::MemoryPool> memory, std::shared_ptr<ResultOwnershipState> ownership,
      std::shared_ptr<void> source) {
  if (!payload.metadata || bytes <= 0 || !memory || !ownership) {
    throw std::invalid_argument("Result delivery lacks its frame or authority");
  }
  owner_ = std::make_shared<detail::ResultDelivery>(
      std::move(payload), bytes, std::move(memory), std::move(ownership), std::move(source));
}

arrow::flight::FlightPayload ResultPayloadQueue::Frame::flightPayload() const {
  if (!owner_) return {};
  auto payload = owner_->payload;
  payload.metadata = std::shared_ptr<arrow::Buffer>(owner_, payload.metadata.get());
  for (auto& body : payload.body_buffers) {
    if (body) body = std::shared_ptr<arrow::Buffer>(owner_, body.get());
  }
  return {nullptr, nullptr, std::move(payload)};
}

ResultPayloadQueue::ResultPayloadQueue(int64_t windowBytes) : window_bytes_(windowBytes) {
  if (windowBytes <= 0) throw std::invalid_argument("Result window must be positive");
}

bool ResultPayloadQueue::hasCredit(int64_t bytes) const {
  // Admit one oversized batch exclusively. The serializer and task pool own
  // the hard wire and memory ceilings.
  return retained_bytes_ == 0 || bytes <= window_bytes_ - retained_bytes_;
}

void ResultPayloadQueue::admit(Frame& frame) {
  frame.owner_->queue = weak_from_this();
  retained_bytes_ += frame.owner_->bytes;
  available_.notify_one();
}

void ResultPayloadQueue::publish(Frame& frame) {
  ready_.emplace_back();
  ready_.back().frame = std::move(frame);
  admit(ready_.back().frame);
}

arrow::Result<bool> ResultPayloadQueue::tryPush(Frame& frame) {
  if (!frame) return arrow::Status::Invalid("Result frame is invalid");
  std::lock_guard<std::mutex> guard(mutex_);
  if (!failure_.ok()) return failure_;
  if (sealed_) return arrow::Status::Invalid("Result producer already completed");
  if (!waiting_.empty() || !hasCredit(frame.owner_->bytes)) return false;
  publish(frame);
  return true;
}

arrow::Status ResultPayloadQueue::pushOrDefer(std::vector<Frame> frames, WakeProducer wake) {
  if (frames.empty() || !wake) return arrow::Status::Invalid("Result publication lacks its frames or waiter");
  std::list<BlockedProducer> pending;
  for (auto& frame : frames) {
    if (!frame) return arrow::Status::Invalid("Result frame is invalid");
    pending.push_back(BlockedProducer{std::move(frame), {}});
  }
  pending.back().wake = std::move(wake);
  {
    std::lock_guard<std::mutex> guard(mutex_);
    if (!failure_.ok()) return failure_;
    if (sealed_) return arrow::Status::Invalid("Result producer already completed");
    waiting_.splice(waiting_.end(), pending);
  }
  promote();
  return arrow::Status::OK();
}

arrow::Result<ResultPayloadQueue::Frame> ResultPayloadQueue::next(
    std::chrono::steady_clock::time_point deadline, const std::function<bool()>& cancelled) {
  std::unique_lock<std::mutex> lock(mutex_);
  while (true) {
    if (!failure_.ok()) return failure_;
    if (cancelled()) return arrow::Status::Cancelled("Result consumer cancelled");
    const auto now = std::chrono::steady_clock::now();
    if (now >= deadline) return arrow::Status::Cancelled("Result read deadline elapsed");
    if (!ready_.empty()) {
      auto next = std::move(ready_.front().frame);
      ready_.pop_front();
      return next;
    }
    if (sealed_) return Frame{};
    available_.wait_until(lock, std::min(deadline, now + std::chrono::milliseconds(100)));
  }
}

arrow::Status ResultPayloadQueue::seal() {
  std::lock_guard<std::mutex> guard(mutex_);
  if (!failure_.ok()) return failure_;
  if (!waiting_.empty()) return arrow::Status::Invalid("Producer completed with blocked result frames");
  sealed_ = true;
  available_.notify_all();
  return arrow::Status::OK();
}

void ResultPayloadQueue::abort(const arrow::Status& failure) {
  if (failure.ok()) throw std::invalid_argument("Result abort requires an error");
  std::list<BlockedProducer> discarded;
  std::list<BlockedProducer> waiting;
  {
    std::lock_guard<std::mutex> guard(mutex_);
    if (!failure_.ok()) return;
    failure_ = failure;
    discarded.swap(ready_);
    waiting.swap(waiting_);
    available_.notify_all();
  }
  discarded.clear();
  for (auto& producer : waiting) {
    producer.frame = Frame{};
    if (producer.wake) producer.wake(failure);
  }
}

void ResultPayloadQueue::release(int64_t bytes) {
  {
    std::lock_guard<std::mutex> guard(mutex_);
    retained_bytes_ -= bytes;
    if (retained_bytes_ < 0) std::terminate();
  }
  promote();
}

void ResultPayloadQueue::promote() {
  while (true) {
    WakeProducer wake;
    {
      std::lock_guard<std::mutex> guard(mutex_);
      if (waiting_.empty() || !hasCredit(waiting_.front().frame.owner_->bytes)) return;
      wake = std::move(waiting_.front().wake);
      ready_.splice(ready_.end(), waiting_, waiting_.begin());
      admit(ready_.back().frame);
    }
    if (wake) wake(arrow::Status::OK());
  }
}

int64_t ResultPayloadQueue::retainedBytes() const {
  std::lock_guard<std::mutex> guard(mutex_);
  return retained_bytes_;
}

}  // namespace pangrova::result
