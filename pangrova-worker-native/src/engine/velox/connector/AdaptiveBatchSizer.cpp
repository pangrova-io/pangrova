// AdaptiveBatchSizer: Adaptive batch size computation and memory-pressure tuning.
// Validates: Requirements R18.1, R18.2, R18.3, R18.4, R18.5

#include "engine/velox/connector/AdaptiveBatchSizer.h"

#include <algorithm>

namespace pangrova::velox {

namespace {

int64_t estimateTypeWidth(const facebook::velox::TypePtr& type) {
  switch (type->kind()) {
    case facebook::velox::TypeKind::BOOLEAN:
      return 1;
    case facebook::velox::TypeKind::TINYINT:
      return 1;
    case facebook::velox::TypeKind::SMALLINT:
      return 2;
    case facebook::velox::TypeKind::INTEGER:
    case facebook::velox::TypeKind::REAL:
      return 4;
    case facebook::velox::TypeKind::BIGINT:
    case facebook::velox::TypeKind::DOUBLE:
    case facebook::velox::TypeKind::TIMESTAMP:
      return 8;
    case facebook::velox::TypeKind::VARCHAR:
    case facebook::velox::TypeKind::VARBINARY:
      return 32;
    default:
      return 64;
  }
}

}  // namespace

int64_t AdaptiveBatchSizer::estimateRowWidth(
    const facebook::velox::RowTypePtr& row_type) {
  if (!row_type || row_type->size() == 0) {
    return 1;
  }
  int64_t total = 0;
  for (size_t i = 0; i < row_type->size(); ++i) {
    total += estimateTypeWidth(row_type->childAt(i));
  }
  return std::max(total, int64_t{1});
}

int64_t AdaptiveBatchSizer::clamp(int64_t value) {
  return std::clamp(value, kMinBatchRows, kMaxBatchRows);
}

int64_t AdaptiveBatchSizer::computeOptimalBatchSize(
    const facebook::velox::RowTypePtr& output_type,
    int64_t target_bytes) {
  if (target_bytes <= 0) {
    target_bytes = kDefaultBatchTargetBytes;
  }
  int64_t row_width = estimateRowWidth(output_type);
  int64_t raw_rows = target_bytes / row_width;
  int64_t clamped = clamp(raw_rows);
  optimal_batch_size_ = clamped;
  current_batch_size_ = clamped;
  return clamped;
}

void AdaptiveBatchSizer::adjustForMemoryPressure(double usage_ratio) {
  if (usage_ratio >= kPressureHighWatermark) {
    // Above high watermark — halve batch size to relieve pressure.
    current_batch_size_ = clamp(current_batch_size_ / 2);
  } else if (usage_ratio < kPressureLowWatermark) {
    // Below low watermark — gradually recover toward optimal.
    int64_t increment = current_batch_size_ / 4;
    if (increment < 1) {
      increment = 1;
    }
    int64_t recovered = current_batch_size_ + increment;
    current_batch_size_ = clamp(std::min(recovered, optimal_batch_size_));
  }
  // Between low and high watermarks (dead zone) — no change.
  // This prevents oscillation when usage hovers near a single threshold.
}

void AdaptiveBatchSizer::calibrate(
    int64_t actual_flat_bytes, int64_t row_count) {
  if (actual_flat_bytes <= 0 || row_count <= 0) {
    return;
  }
  int64_t actual_row_width = actual_flat_bytes / row_count;
  if (actual_row_width <= 0) {
    return;
  }
  int64_t raw_rows = kDefaultBatchTargetBytes / actual_row_width;
  optimal_batch_size_ = clamp(raw_rows);
  current_batch_size_ = optimal_batch_size_;
}

void AdaptiveBatchSizer::adjustForIOLatency(int64_t latency_ms) {
  // L2-7: IO latency-aware batch size scaling.
  // High latency storage (S3, GCS — typically >50ms per request) benefits
  // from larger batches to amortize per-request overhead. Local/SSD storage
  // (<5ms) uses smaller batches for lower per-query latency.
  if (latency_ms >= kHighLatencyThresholdMs) {
    // Scale up batch size to fill the latency budget — each IO round-trip
    // is expensive, so reading more rows per call reduces total calls.
    int64_t scaled = static_cast<int64_t>(
        current_batch_size_ * kLatencyScaleFactor);
    current_batch_size_ = clamp(std::min(scaled, kMaxBatchRows));
  } else if (latency_ms <= kLowLatencyThresholdMs &&
             current_batch_size_ > optimal_batch_size_) {
    // IO is fast — revert toward optimal to reduce per-batch processing time.
    current_batch_size_ = optimal_batch_size_;
  }
  // Between thresholds: no change (hysteresis).
}

}  // namespace pangrova::velox
