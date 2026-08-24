// VectorTopKHeap: Bounded max-heap for Top-K nearest neighbor accumulation.
//
// Maintains the K smallest distances seen so far using a max-heap.
// The heap root always holds the WORST (largest distance) of the current
// K best candidates, enabling O(1) threshold comparison and O(log K) insert.
//
// Thread-safe: not thread-safe — callers must synchronize.
// Placement: vector/ — Native vector index computation components.
#pragma once

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <vector>

namespace pangrova::vector {

struct ScoredRow {
  int64_t rowId;
  double distance;
};

class VectorTopKHeap {
 public:
  explicit VectorTopKHeap(size_t k) : k_(k) {
    heap_.reserve(k + 1);
  }

  void push(int64_t rowId, double distance) {
    if (heap_.size() < k_) {
      heap_.push_back({rowId, distance});
      std::push_heap(heap_.begin(), heap_.end(), cmp_);
    } else if (distance < heap_.front().distance) {
      std::pop_heap(heap_.begin(), heap_.end(), cmp_);
      heap_.back() = {rowId, distance};
      std::push_heap(heap_.begin(), heap_.end(), cmp_);
    }
  }

  double threshold() const {
    if (heap_.size() < k_) {
      return std::numeric_limits<double>::max();
    }
    return heap_.front().distance;
  }

  bool full() const { return heap_.size() >= k_; }

  size_t size() const { return heap_.size(); }

  std::vector<ScoredRow> drainSorted() {
    std::sort_heap(heap_.begin(), heap_.end(), cmp_);
    auto result = std::move(heap_);
    heap_.clear();
    return result;
  }

 private:
  size_t k_;
  std::vector<ScoredRow> heap_;

  struct MaxDistanceComparator {
    bool operator()(const ScoredRow& a, const ScoredRow& b) const {
      return a.distance < b.distance;
    }
  };

  static constexpr MaxDistanceComparator cmp_{};
};

}  // namespace pangrova::vector
