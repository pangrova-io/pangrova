#pragma once

#include <condition_variable>
#include <cstdint>
#include <memory>
#include <mutex>

namespace pangrova::resource {

/** One admitted task grant. Counts backend requested ranges, including failed attempts. */
class TaskObjectReadBudget final : public std::enable_shared_from_this<TaskObjectReadBudget> {
 public:
  class Attempt final {
   public:
    ~Attempt();
    Attempt(const Attempt&) = delete;
    Attempt& operator=(const Attempt&) = delete;
    void complete(uint64_t bytes);
   private:
    friend class TaskObjectReadBudget;
    Attempt() = default;
    std::shared_ptr<TaskObjectReadBudget> owner_;
    bool completed_ = false;
  };
  explicit TaskObjectReadBudget(uint64_t limit) : limit_(limit) {}
  std::shared_ptr<Attempt> begin(uint64_t requested_bytes);
  void requestCancel();
  void stopAndDrain();
  uint64_t requestedBytes() const;
  // SDK-reported return sizes, which may include logical gaps. Not wire bytes.
  uint64_t completedReturnedBytes() const;
  uint64_t activeReads() const;
 private:
  const uint64_t limit_;
  mutable std::mutex mutex_;
  std::condition_variable drained_;
  uint64_t requested_ = 0;
  uint64_t completed_ = 0;
  uint64_t active_ = 0;
  bool stopped_ = false;
};

}  // namespace pangrova::resource
