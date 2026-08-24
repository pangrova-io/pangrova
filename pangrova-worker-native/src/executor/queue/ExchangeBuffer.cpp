#include "executor/queue/ExchangeBuffer.h"

namespace pangrova::executor {

ExchangeBuffer::ExchangeBuffer(size_t per_source_capacity)
    : per_source_capacity_(per_source_capacity) {}

bool ExchangeBuffer::addBatch(
    const std::string& source_id,
    facebook::velox::RowVectorPtr row_vector) {
  std::lock_guard<std::mutex> lock(mutex_);
  if (closed_ || completed_sources_.count(source_id) > 0) {
    return false;
  }
  auto& queue = buffers_[source_id];
  // Track source insertion order for round-robin polling.
  // If operator[] just default-constructed a new entry, source_order_
  // is one behind buffers_ — append the new source_id.
  if (source_order_.size() < buffers_.size()) {
    source_order_.push_back(source_id);
  }
  if (queue.size() >= per_source_capacity_) {
    return false;  // Backpressure — caller should retry.
  }
  queue.push(std::move(row_vector));
  not_empty_.notify_one();
  return true;
}

facebook::velox::RowVectorPtr ExchangeBuffer::pollBatch(
    std::chrono::milliseconds timeout) {
  std::unique_lock<std::mutex> lock(mutex_);
  not_empty_.wait_for(lock, timeout, [this] {
    if (closed_ || allSourcesDrained()) return true;
    for (const auto& [id, queue] : buffers_) {
      if (!queue.empty()) return true;
    }
    return false;
  });
  // P3-BUG-23: Round-robin scan starting from poll_index_ to ensure
  // fair consumption across sources. Without this, unordered_map iteration
  // order (hash-dependent) can starve sources whose keys hash later.
  if (!source_order_.empty()) {
    size_t n = source_order_.size();
    for (size_t i = 0; i < n; ++i) {
      size_t idx = (poll_index_ + i) % n;
      auto it = buffers_.find(source_order_[idx]);
      if (it != buffers_.end() && !it->second.empty()) {
        auto batch = std::move(it->second.front());
        it->second.pop();
        poll_index_ = (idx + 1) % n;
        return batch;
      }
    }
  }
  return nullptr;
}

void ExchangeBuffer::markSourceComplete(const std::string& source_id) {
  std::lock_guard<std::mutex> lock(mutex_);
  completed_sources_.insert(source_id);
  not_empty_.notify_all();
}

void ExchangeBuffer::markNoMoreSources() {
  std::lock_guard<std::mutex> lock(mutex_);
  no_more_sources_ = true;
  not_empty_.notify_all();
}

bool ExchangeBuffer::isFinished() const {
  std::lock_guard<std::mutex> lock(mutex_);
  return no_more_sources_ && allSourcesDrained();
}

size_t ExchangeBuffer::totalBuffered() const {
  std::lock_guard<std::mutex> lock(mutex_);
  size_t total = 0;
  for (const auto& [id, queue] : buffers_) {
    total += queue.size();
  }
  return total;
}

size_t ExchangeBuffer::sourceCount() const {
  std::lock_guard<std::mutex> lock(mutex_);
  return buffers_.size();
}

void ExchangeBuffer::close() {
  std::lock_guard<std::mutex> lock(mutex_);
  closed_ = true;
  not_empty_.notify_all();
}

bool ExchangeBuffer::allSourcesDrained() const {
  if (!no_more_sources_) {
    return false;
  }
  for (const auto& [id, queue] : buffers_) {
    if (!queue.empty()) {
      return false;
    }
    if (completed_sources_.count(id) == 0) {
      return false;
    }
  }
  return true;
}

}  // namespace pangrova::executor
