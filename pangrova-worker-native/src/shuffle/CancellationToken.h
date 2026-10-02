#pragma once

#include <atomic>
#include <memory>

namespace pangrova::shuffle {

// Portable cancellation primitive for supported C++20 toolchains whose
// standard libraries do not yet expose std::stop_token.
class CancellationToken final {
 public:
  CancellationToken() = default;
  bool stopRequested() const noexcept {
    return state_ && state_->load(std::memory_order_acquire);
  }

 private:
  explicit CancellationToken(std::shared_ptr<std::atomic<bool>> state)
      : state_(std::move(state)) {}
  std::shared_ptr<std::atomic<bool>> state_;
  friend class CancellationSource;
};

class CancellationSource final {
 public:
  CancellationSource() : state_(std::make_shared<std::atomic<bool>>(false)) {}
  void requestStop() noexcept {
    state_->store(true, std::memory_order_release);
  }
  CancellationToken token() const noexcept { return CancellationToken(state_); }

 private:
  std::shared_ptr<std::atomic<bool>> state_;
};

}  // namespace pangrova::shuffle
