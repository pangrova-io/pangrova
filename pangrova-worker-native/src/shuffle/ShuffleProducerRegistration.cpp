#include "shuffle/ShuffleProducerRegistration.h"

#include <stdexcept>

#include "observability/PangrovaLog.h"
#include "shuffle/FlightShuffleServer.h"

namespace pangrova::shuffle {

ShuffleProducerRegistration::ShuffleProducerRegistration(ShuffleProducerIdentity identity)
    : identity_(std::move(identity)) {
  if (identity_.handle <= 0 || !identity_.queue || identity_.query_id.empty() ||
      identity_.stage_id < 0 || identity_.partition_count <= 0 ||
      identity_.plan_epoch == 0 || identity_.fencing_token == 0) {
    throw std::invalid_argument("shuffle registration requires exact fenced producer identity");
  }
}

ShuffleProducerRegistration::~ShuffleProducerRegistration() {
  // No other caller can own an unpublished STARTING registration when its
  // final reference is destroyed (for example, failed map allocation).
  {
    std::lock_guard<std::mutex> lock(mutex_);
    if (state_ == State::STARTING) state_ = State::CLOSED;
  }
  try {
    close(true);
  } catch (const std::exception& error) {
    PANGROVA_LOG(ERROR) << "event=shuffle_registration_destructor_close_failed"
                       << " handle=" << identity_.handle << " error=" << error.what();
  }
}

void ShuffleProducerRegistration::publish(std::shared_ptr<PartitionDemuxer> demuxer) {
  std::lock_guard<std::mutex> lock(mutex_);
  demuxer_ = std::move(demuxer);
  state_ = State::ACTIVE;
  cv_.notify_all();
}

void ShuffleProducerRegistration::failStart(std::exception_ptr error) {
  std::lock_guard<std::mutex> lock(mutex_);
  startup_error_ = std::move(error);
  state_ = State::CLOSED;
  cv_.notify_all();
}

void ShuffleProducerRegistration::awaitActive() const {
  std::unique_lock<std::mutex> lock(mutex_);
  cv_.wait(lock, [this] { return state_ != State::STARTING; });
  if (startup_error_) std::rethrow_exception(startup_error_);
  if (state_ != State::ACTIVE) throw std::logic_error("shuffle producer is no longer active");
  auto demuxer = demuxer_;
  lock.unlock();
  const auto status = demuxer->status();
  if (!status.ok()) throw std::runtime_error("shuffle producer failed: " + status.ToString());
}

ShuffleProducerRegistration::State ShuffleProducerRegistration::state() const {
  std::lock_guard<std::mutex> lock(mutex_);
  return state_;
}

ShuffleProducerRegistration::Snapshot ShuffleProducerRegistration::snapshot() const {
  std::lock_guard<std::mutex> lock(mutex_);
  return {identity_, demuxer_, state_};
}

void ShuffleProducerRegistration::close(bool force) {
  std::shared_ptr<PartitionDemuxer> demuxer;
  {
    std::unique_lock<std::mutex> lock(mutex_);
    cv_.wait(lock, [this] { return state_ != State::STARTING && state_ != State::DRAINING; });
    if (state_ == State::CLOSED || (state_ == State::LINGERING && !force)) return;
    state_ = State::DRAINING;
    demuxer = demuxer_;
  }
  try {
    const bool clean = !force && demuxer->quiesce(std::chrono::milliseconds(500));
    const bool linger = clean && !(demuxer->isFullyConsumed() &&
                                   demuxer->allPartitionsReadComplete());
    if (!linger) demuxer->stop();
    std::lock_guard<std::mutex> lock(mutex_);
    state_ = linger ? State::LINGERING : State::CLOSED;
    if (linger) entered_linger_ = std::chrono::steady_clock::now();
    else demuxer_.reset();  // The local owner defers destruction until after unlock.
    cv_.notify_all();
  } catch (...) {
    std::lock_guard<std::mutex> lock(mutex_);
    state_ = State::CLOSE_FAILED;
    cv_.notify_all();
    throw;
  }
}

bool ShuffleProducerRegistration::shouldEvict(
    std::chrono::steady_clock::time_point now, std::chrono::milliseconds ttl) const {
  std::shared_ptr<PartitionDemuxer> demuxer;
  std::chrono::steady_clock::time_point entered;
  {
    std::lock_guard<std::mutex> lock(mutex_);
    if (state_ == State::CLOSED) return true;
    if (state_ != State::LINGERING) return false;
    demuxer = demuxer_;
    entered = entered_linger_;
  }
  return now - entered >= ttl ||
      (demuxer->isFullyConsumed() && demuxer->allPartitionsReadComplete());
}

}  // namespace pangrova::shuffle
