// AdaptiveBatchSizer: Adaptive batch size tuning for data sources.
// Computes optimal row count based on output schema column widths and a
// configurable target byte budget. Adjusts dynamically under memory pressure
// (halve) and recovers gradually (25% increase) when pressure is relieved.
//
// Thread-safety: NOT thread-safe. Intended for single-threaded use within
// one PangrovaVeloxDataSource instance.
//
// Placement: velox/ — Velox integration utilities.
// Validates: Requirements R18.1, R18.2, R18.3, R18.4, R18.5
#pragma once

#include <cstdint>

#include "velox/type/Type.h"

namespace pangrova::velox {

inline constexpr int64_t kMinBatchRows = 256;
inline constexpr int64_t kMaxBatchRows = 8192;
inline constexpr int64_t kDefaultBatchTargetBytes = 4L * 1024 * 1024;

// Memory pressure hysteresis thresholds to prevent batch size oscillation.
// Batch size halves when usage rises above kPressureHighWatermark and
// recovers (25% increase) only when usage drops below kPressureLowWatermark.
inline constexpr double kPressureHighWatermark = 0.85;
inline constexpr double kPressureLowWatermark = 0.70;

// L2-7: IO latency thresholds for batch size scaling.
// When IO latency exceeds kHighLatencyThresholdMs (remote storage like S3),
// batch size is increased by kLatencyScaleFactor to amortize round-trip cost.
inline constexpr int64_t kHighLatencyThresholdMs = 50;
inline constexpr double kLatencyScaleFactor = 2.0;
inline constexpr int64_t kLowLatencyThresholdMs = 5;

// Stateful batch sizer that tracks current batch size and adapts to
// memory pressure signals.
class AdaptiveBatchSizer {
 public:
  AdaptiveBatchSizer() = default;

  int64_t computeOptimalBatchSize(
      const facebook::velox::RowTypePtr& output_type,
      int64_t target_bytes = kDefaultBatchTargetBytes);

  // Adjust batch size based on memory usage ratio with hysteresis.
  // Halves batch size when usage exceeds kPressureHighWatermark (0.85).
  // Recovers (+25%) only when usage drops below kPressureLowWatermark (0.70).
  // The dead zone between 0.70–0.85 prevents oscillation.
  void adjustForMemoryPressure(double usage_ratio);

  // L2-7: Adjust batch size based on IO latency.
  // High latency (>50ms, e.g., S3) increases batch size to amortize overhead.
  // Low latency (<5ms, e.g., local SSD) keeps smaller batches for responsiveness.
  void adjustForIOLatency(int64_t latency_ms);

  // Calibrate batch size using actual row width from a real batch.
  // Called after the first batch is read to replace the schema-based
  // estimate (which uses fixed 32 bytes for VARCHAR) with the actual
  // average row width. No-op if actual_flat_bytes or row_count is zero.
  void calibrate(int64_t actual_flat_bytes, int64_t row_count);

  int64_t getCurrentBatchSize() const { return current_batch_size_; }
  int64_t getOptimalBatchSize() const { return optimal_batch_size_; }

  static int64_t estimateRowWidth(const facebook::velox::RowTypePtr& row_type);

 private:
  static int64_t clamp(int64_t value);

  int64_t optimal_batch_size_ = kMinBatchRows;
  int64_t current_batch_size_ = kMinBatchRows;
};

}  // namespace pangrova::velox
