#pragma once

#include <condition_variable>
#include <cstdint>
#include <memory>
#include <mutex>

namespace pangrova::resource {

/** One admitted task grant. Counts backend requested ranges, including failed attempts. */
class TaskObjectReadBudget final : public std::enable_shared_from_this<TaskObjectReadBudget> {
 public:
  enum class Operation { PAYLOAD, METADATA };
  struct Counters {
    uint64_t requested_bytes = 0;
    uint64_t returned_bytes = 0;
    uint64_t payload_calls = 0;
    uint64_t metadata_calls = 0;
    uint64_t failed_calls = 0;
  };

  // Each owner issues calls serially and publishes their outcomes when its
  // operation exits, under the same lock that releases the drain fence.
  class Attempt final {
   public:
    ~Attempt();
    Attempt(Attempt&&) noexcept = default;
    Attempt(const Attempt&) = delete;
    Attempt& operator=(const Attempt&) = delete;
    void startCall() noexcept { ++calls_; }
    void completeCall(uint64_t bytes);
   private:
    friend class TaskObjectReadBudget;
    Attempt(std::shared_ptr<TaskObjectReadBudget> owner, uint64_t bytes, Operation operation) noexcept;
    std::shared_ptr<TaskObjectReadBudget> owner_;
    uint64_t reserved_bytes_;
    Operation operation_;
    uint64_t calls_ = 0;
    uint64_t completed_calls_ = 0;
    uint64_t returned_bytes_ = 0;
  };
  explicit TaskObjectReadBudget(uint64_t limit) : limit_(limit) {}
  Attempt begin(uint64_t requested_bytes, Operation operation = Operation::PAYLOAD);
  void requestCancel();
  void stopAndDrain();
  // A coherent snapshot of admitted reservations and finished adapter operations.
  Counters snapshot() const;
  uint64_t requestedBytes() const { return snapshot().requested_bytes; }
  uint64_t completedReturnedBytes() const { return snapshot().returned_bytes; }
  uint64_t activeReads() const;
 private:
  const uint64_t limit_;
  mutable std::mutex mutex_;
  std::condition_variable drained_;
  Counters counters_;
  uint64_t active_ = 0;
  bool stopped_ = false;
};

}  // namespace pangrova::resource
