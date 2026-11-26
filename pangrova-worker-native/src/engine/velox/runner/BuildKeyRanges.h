#pragma once

#include <algorithm>
#include <array>
#include <cstddef>
#include <cstdint>
#include <span>

namespace pangrova::velox {

struct BuildKeyRange {
  int64_t minimum;
  int64_t maximum;
};

/** A conservative union that bounds the work and size of one task's build-key report. */
class BuildKeyRanges {
 public:
  static constexpr std::size_t kMaximumRanges = 16;

  void add(int64_t value) {
    addRange({value, value});
  }

  void merge(const BuildKeyRanges& other) {
    for (const auto& range : other.ranges()) {
      addRange(range);
    }
  }

  std::span<const BuildKeyRange> ranges() const {
    return {ranges_.data(), size_};
  }

 private:
  static bool separated(const BuildKeyRange& left, const BuildKeyRange& right) {
    return left.maximum < right.minimum && left.maximum + 1 < right.minimum;
  }

  void addRange(BuildKeyRange candidate) {
    if (collapsed_) {
      ranges_[0] = {std::min(ranges_[0].minimum, candidate.minimum),
                    std::max(ranges_[0].maximum, candidate.maximum)};
      return;
    }

    std::size_t first = 0;
    while (first < size_ && separated(ranges_[first], candidate)) {
      ++first;
    }
    std::size_t end = first;
    while (end < size_ && !separated(candidate, ranges_[end])) {
      const auto& current = ranges_[end++];
      candidate.minimum = std::min(candidate.minimum, current.minimum);
      candidate.maximum = std::max(candidate.maximum, current.maximum);
    }
    if (first == end) {
      if (size_ == kMaximumRanges) {
        ranges_[0] = {std::min(ranges_.front().minimum, candidate.minimum),
                      std::max(ranges_.back().maximum, candidate.maximum)};
        size_ = 1;
        collapsed_ = true;
        return;
      }
      std::move_backward(ranges_.begin() + first, ranges_.begin() + size_, ranges_.begin() + size_ + 1);
      ++size_;
    } else if (end > first + 1) {
      std::move(ranges_.begin() + end, ranges_.begin() + size_, ranges_.begin() + first + 1);
      size_ -= end - first - 1;
    }
    ranges_[first] = candidate;
  }

  std::array<BuildKeyRange, kMaximumRanges> ranges_{};
  std::size_t size_{0};
  bool collapsed_{false};
};

}  // namespace pangrova::velox
