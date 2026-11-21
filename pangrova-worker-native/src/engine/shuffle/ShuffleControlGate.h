#pragma once

#include <cstddef>
#include <condition_variable>
#include <mutex>
#include <stdexcept>
#include <utility>

namespace pangrova::shuffle {

// Metadata-only admission for actual shuffle control operations. A lease never
// holds the mutex while its owner performs I/O, waits or joins a thread.
class ShuffleControlGate {
 public:
  class Lease {
   public:
    Lease() = default;
    Lease(const Lease&) = delete;
    Lease& operator=(const Lease&) = delete;
    Lease(Lease&& other) noexcept : gate_(std::exchange(other.gate_, nullptr)) {}
    Lease& operator=(Lease&& other) noexcept {
      if (this != &other) {
        reset();
        gate_ = std::exchange(other.gate_, nullptr);
      }
      return *this;
    }
    ~Lease() { reset(); }
    void reset() {
      if (auto* gate = std::exchange(gate_, nullptr)) gate->leave();
    }
   private:
    friend class ShuffleControlGate;
    explicit Lease(ShuffleControlGate* gate) : gate_(gate) {}
    ShuffleControlGate* gate_ = nullptr;
  };

  Lease acquire() {
    std::lock_guard<std::mutex> lock(mutex_);
    if (sealed_) throw std::logic_error("shuffle control owner is sealed");
    ++active_;
    return Lease(this);
  }
  void sealAndWait() {
    std::unique_lock<std::mutex> lock(mutex_);
    sealed_ = true;
    cv_.notify_all();
    cv_.wait(lock, [this] { return active_ == 0; });
  }
  bool isSealed() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return sealed_;
  }
  bool owns(const Lease& lease) const { return lease.gate_ == this; }
 private:
  void leave() {
    std::lock_guard<std::mutex> lock(mutex_);
    --active_;
    cv_.notify_all();
  }
  mutable std::mutex mutex_;
  std::condition_variable cv_;
  std::size_t active_ = 0;
  bool sealed_ = false;
};

}  // namespace pangrova::shuffle
