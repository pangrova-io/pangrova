#include "engine/resource/TaskObjectReadBudget.h"

#include <limits>
#include <stdexcept>

namespace pangrova::resource {

std::shared_ptr<TaskObjectReadBudget::Attempt> TaskObjectReadBudget::begin(uint64_t bytes) {
  auto owner = shared_from_this();
  auto attempt = std::shared_ptr<Attempt>(new Attempt());
  std::lock_guard<std::mutex> lock(mutex_);
  if (stopped_) throw std::runtime_error("Task object read cancelled or released");
  if (bytes > limit_ - requested_) {
    throw std::runtime_error("Task object-store requested-read grant exhausted");
  }
  requested_ += bytes;
  ++active_;
  attempt->owner_ = std::move(owner);
  return attempt;
}

TaskObjectReadBudget::Attempt::~Attempt() {
  if (!owner_) return;
  std::lock_guard<std::mutex> lock(owner_->mutex_);
  --owner_->active_;
  if (owner_->active_ == 0) owner_->drained_.notify_all();
}

void TaskObjectReadBudget::Attempt::complete(uint64_t bytes) {
  std::lock_guard<std::mutex> lock(owner_->mutex_);
  if (completed_ || bytes > std::numeric_limits<uint64_t>::max() - owner_->completed_) {
    throw std::runtime_error("Task object read completion is duplicated or overflows");
  }
  owner_->completed_ += bytes;
  completed_ = true;
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

uint64_t TaskObjectReadBudget::requestedBytes() const {
  std::lock_guard<std::mutex> lock(mutex_);
  return requested_;
}
uint64_t TaskObjectReadBudget::completedReturnedBytes() const {
  std::lock_guard<std::mutex> lock(mutex_);
  return completed_;
}
uint64_t TaskObjectReadBudget::activeReads() const {
  std::lock_guard<std::mutex> lock(mutex_);
  return active_;
}

}  // namespace pangrova::resource
