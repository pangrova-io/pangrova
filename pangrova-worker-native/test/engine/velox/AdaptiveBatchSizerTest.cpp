// AdaptiveBatchSizerTest: Unit tests for adaptive batch size tuning.
// Validates: Requirements R18.1, R18.2, R18.3, R18.4, R18.5

#include <gtest/gtest.h>

#include "engine/velox/connector/AdaptiveBatchSizer.h"

#include "velox/type/Type.h"

namespace pangrova::velox {
namespace {

using namespace facebook::velox;

// ---------------------------------------------------------------------------
// estimateRowWidth
// ---------------------------------------------------------------------------

TEST(AdaptiveBatchSizerTest, EstimateRowWidthFixedTypes) {
  // 3 columns: INTEGER(4) + BIGINT(8) + DOUBLE(8) = 20 bytes/row
  auto row_type = ROW({"a", "b", "c"}, {INTEGER(), BIGINT(), DOUBLE()});
  EXPECT_EQ(AdaptiveBatchSizer::estimateRowWidth(row_type), 20);
}

TEST(AdaptiveBatchSizerTest, EstimateRowWidthWithVarchar) {
  // INTEGER(4) + VARCHAR(32) = 36 bytes/row
  auto row_type = ROW({"id", "name"}, {INTEGER(), VARCHAR()});
  EXPECT_EQ(AdaptiveBatchSizer::estimateRowWidth(row_type), 36);
}

TEST(AdaptiveBatchSizerTest, EstimateRowWidthSmallTypes) {
  // BOOLEAN(1) + TINYINT(1) + SMALLINT(2) = 4 bytes/row
  auto row_type = ROW({"a", "b", "c"}, {BOOLEAN(), TINYINT(), SMALLINT()});
  EXPECT_EQ(AdaptiveBatchSizer::estimateRowWidth(row_type), 4);
}

TEST(AdaptiveBatchSizerTest, EstimateRowWidthEmptySchema) {
  auto row_type = ROW({}, {});
  // Degenerate case: returns 1 to avoid division by zero.
  EXPECT_EQ(AdaptiveBatchSizer::estimateRowWidth(row_type), 1);
}

TEST(AdaptiveBatchSizerTest, EstimateRowWidthNullType) {
  EXPECT_EQ(AdaptiveBatchSizer::estimateRowWidth(nullptr), 1);
}

TEST(AdaptiveBatchSizerTest, EstimateRowWidthComplexType) {
  // ARRAY type gets conservative 64-byte estimate.
  auto row_type = ROW({"arr"}, {ARRAY(INTEGER())});
  EXPECT_EQ(AdaptiveBatchSizer::estimateRowWidth(row_type), 64);
}

// ---------------------------------------------------------------------------
// computeOptimalBatchSize — R18.1
// ---------------------------------------------------------------------------

TEST(AdaptiveBatchSizerTest, ComputeOptimalNarrowSchema) {
  AdaptiveBatchSizer sizer;
  // 3 INTEGER columns = 12 bytes/row. 4MB / 12 = 349525 → clamped to 8192.
  auto row_type = ROW({"a", "b", "c"}, {INTEGER(), INTEGER(), INTEGER()});
  auto result = sizer.computeOptimalBatchSize(row_type);
  EXPECT_EQ(result, kMaxBatchRows);
  EXPECT_EQ(sizer.getCurrentBatchSize(), kMaxBatchRows);
  EXPECT_EQ(sizer.getOptimalBatchSize(), kMaxBatchRows);
}

TEST(AdaptiveBatchSizerTest, ComputeOptimalWideSchema) {
  AdaptiveBatchSizer sizer;
  // 20 VARCHAR columns = 20 * 32 = 640 bytes/row. 4MB / 640 = 6553.
  std::vector<std::string> names;
  std::vector<TypePtr> types;
  for (int i = 0; i < 20; ++i) {
    names.push_back("col_" + std::to_string(i));
    types.push_back(VARCHAR());
  }
  auto row_type = ROW(std::move(names), std::move(types));
  auto result = sizer.computeOptimalBatchSize(row_type);
  EXPECT_EQ(result, 6553);
  EXPECT_GE(result, kMinBatchRows);
  EXPECT_LE(result, kMaxBatchRows);
}

TEST(AdaptiveBatchSizerTest, ComputeOptimalVeryWideSchema) {
  AdaptiveBatchSizer sizer;
  // 100 VARCHAR columns = 3200 bytes/row. 4MB / 3200 = 1310.
  std::vector<std::string> names;
  std::vector<TypePtr> types;
  for (int i = 0; i < 100; ++i) {
    names.push_back("col_" + std::to_string(i));
    types.push_back(VARCHAR());
  }
  auto row_type = ROW(std::move(names), std::move(types));
  auto result = sizer.computeOptimalBatchSize(row_type);
  EXPECT_EQ(result, 1310);
}

TEST(AdaptiveBatchSizerTest, ComputeOptimalExtremelyWideSchema) {
  AdaptiveBatchSizer sizer;
  // 500 VARCHAR columns = 16000 bytes/row. 4MB / 16000 = 262.
  std::vector<std::string> names;
  std::vector<TypePtr> types;
  for (int i = 0; i < 500; ++i) {
    names.push_back("col_" + std::to_string(i));
    types.push_back(VARCHAR());
  }
  auto row_type = ROW(std::move(names), std::move(types));
  auto result = sizer.computeOptimalBatchSize(row_type);
  EXPECT_EQ(result, 262);
}

TEST(AdaptiveBatchSizerTest, ComputeOptimalCustomTargetBytes) {
  AdaptiveBatchSizer sizer;
  // 1 BIGINT column = 8 bytes/row. 4096 bytes / 8 = 512.
  auto row_type = ROW({"id"}, {BIGINT()});
  auto result = sizer.computeOptimalBatchSize(row_type, 4096);
  EXPECT_EQ(result, 512);
}

// R18.4: Boundary clamping
TEST(AdaptiveBatchSizerTest, ComputeOptimalClampedToMin) {
  AdaptiveBatchSizer sizer;
  // Very small target_bytes forces result below min.
  // 1 BIGINT = 8 bytes/row. 100 bytes / 8 = 12 → clamped to 256.
  auto row_type = ROW({"id"}, {BIGINT()});
  auto result = sizer.computeOptimalBatchSize(row_type, 100);
  EXPECT_EQ(result, kMinBatchRows);
}

TEST(AdaptiveBatchSizerTest, ComputeOptimalClampedToMax) {
  AdaptiveBatchSizer sizer;
  // 1 BOOLEAN = 1 byte/row. 4MB / 1 = 4194304 → clamped to 8192.
  auto row_type = ROW({"flag"}, {BOOLEAN()});
  auto result = sizer.computeOptimalBatchSize(row_type);
  EXPECT_EQ(result, kMaxBatchRows);
}

TEST(AdaptiveBatchSizerTest, ComputeOptimalNegativeTargetBytesUsesDefault) {
  AdaptiveBatchSizer sizer;
  auto row_type = ROW({"id"}, {BIGINT()});
  auto result = sizer.computeOptimalBatchSize(row_type, -1);
  // Should use default 4MB. 4MB / 8 = 524288 → clamped to 8192.
  EXPECT_EQ(result, kMaxBatchRows);
}

// ---------------------------------------------------------------------------
// adjustForMemoryPressure — R18.2, R18.3
// ---------------------------------------------------------------------------

TEST(AdaptiveBatchSizerTest, MemoryPressureHalvesBatchSize) {
  AdaptiveBatchSizer sizer;
  auto row_type = ROW({"a", "b"}, {BIGINT(), BIGINT()});
  // 16 bytes/row. 4MB / 16 = 262144 → clamped to 8192.
  sizer.computeOptimalBatchSize(row_type);
  EXPECT_EQ(sizer.getCurrentBatchSize(), 8192);

  sizer.adjustForMemoryPressure(0.90);
  EXPECT_EQ(sizer.getCurrentBatchSize(), 4096);

  sizer.adjustForMemoryPressure(0.90);
  EXPECT_EQ(sizer.getCurrentBatchSize(), 2048);
}

TEST(AdaptiveBatchSizerTest, MemoryPressureFloorsAtMin) {
  AdaptiveBatchSizer sizer;
  auto row_type = ROW({"id"}, {INTEGER()});
  sizer.computeOptimalBatchSize(row_type, 2048);
  // 4 bytes/row. 2048 / 4 = 512.
  EXPECT_EQ(sizer.getCurrentBatchSize(), 512);

  sizer.adjustForMemoryPressure(0.90);  // 256
  EXPECT_EQ(sizer.getCurrentBatchSize(), 256);

  sizer.adjustForMemoryPressure(0.90);  // 128 → clamped to 256
  EXPECT_EQ(sizer.getCurrentBatchSize(), kMinBatchRows);
}

TEST(AdaptiveBatchSizerTest, RecoveryIncreasesBy25Percent) {
  AdaptiveBatchSizer sizer;
  auto row_type = ROW({"a", "b"}, {BIGINT(), BIGINT()});
  sizer.computeOptimalBatchSize(row_type);
  // optimal = 8192, current = 8192.

  // Halve twice: 8192 → 4096 → 2048.
  sizer.adjustForMemoryPressure(0.90);
  sizer.adjustForMemoryPressure(0.90);
  EXPECT_EQ(sizer.getCurrentBatchSize(), 2048);

  // Recover: 2048 + 512 = 2560.
  sizer.adjustForMemoryPressure(0.50);
  EXPECT_EQ(sizer.getCurrentBatchSize(), 2560);

  // Recover: 2560 + 640 = 3200.
  sizer.adjustForMemoryPressure(0.50);
  EXPECT_EQ(sizer.getCurrentBatchSize(), 3200);
}

TEST(AdaptiveBatchSizerTest, RecoveryCapsAtOptimal) {
  AdaptiveBatchSizer sizer;
  // Compute optimal = 1000 (custom target).
  // 1 BIGINT = 8 bytes/row. 8000 / 8 = 1000.
  auto row_type = ROW({"id"}, {BIGINT()});
  sizer.computeOptimalBatchSize(row_type, 8000);
  EXPECT_EQ(sizer.getCurrentBatchSize(), 1000);

  // Halve: 500.
  sizer.adjustForMemoryPressure(0.90);
  EXPECT_EQ(sizer.getCurrentBatchSize(), 500);

  // Recover: 500 + 125 = 625.
  sizer.adjustForMemoryPressure(0.50);
  EXPECT_EQ(sizer.getCurrentBatchSize(), 625);

  // Recover: 625 + 156 = 781.
  sizer.adjustForMemoryPressure(0.50);
  EXPECT_EQ(sizer.getCurrentBatchSize(), 781);

  // Recover: 781 + 195 = 976.
  sizer.adjustForMemoryPressure(0.50);
  EXPECT_EQ(sizer.getCurrentBatchSize(), 976);

  // Recover: 976 + 244 = 1220 → capped at optimal 1000.
  sizer.adjustForMemoryPressure(0.50);
  EXPECT_EQ(sizer.getCurrentBatchSize(), 1000);
}

TEST(AdaptiveBatchSizerTest, RecoveryDoesNotExceedMax) {
  AdaptiveBatchSizer sizer;
  // Already at max.
  auto row_type = ROW({"id"}, {BOOLEAN()});
  sizer.computeOptimalBatchSize(row_type);
  EXPECT_EQ(sizer.getCurrentBatchSize(), kMaxBatchRows);

  // Recovery should be a no-op at max.
  sizer.adjustForMemoryPressure(0.50);
  EXPECT_EQ(sizer.getCurrentBatchSize(), kMaxBatchRows);
}

// ---------------------------------------------------------------------------
// Hysteresis dead zone — usage between low and high watermarks
// ---------------------------------------------------------------------------

TEST(AdaptiveBatchSizerTest, DeadZoneNoChange) {
  AdaptiveBatchSizer sizer;
  auto row_type = ROW({"a", "b"}, {BIGINT(), BIGINT()});
  sizer.computeOptimalBatchSize(row_type);

  // Halve once: 8192 → 4096.
  sizer.adjustForMemoryPressure(0.90);
  EXPECT_EQ(sizer.getCurrentBatchSize(), 4096);

  // Usage in dead zone (0.70 ≤ usage < 0.85) — no change.
  sizer.adjustForMemoryPressure(0.75);
  EXPECT_EQ(sizer.getCurrentBatchSize(), 4096);

  sizer.adjustForMemoryPressure(0.80);
  EXPECT_EQ(sizer.getCurrentBatchSize(), 4096);

  sizer.adjustForMemoryPressure(0.84);
  EXPECT_EQ(sizer.getCurrentBatchSize(), 4096);
}

// ---------------------------------------------------------------------------
// Default state before computeOptimalBatchSize
// ---------------------------------------------------------------------------

TEST(AdaptiveBatchSizerTest, DefaultBatchSizeIsMin) {
  AdaptiveBatchSizer sizer;
  EXPECT_EQ(sizer.getCurrentBatchSize(), kMinBatchRows);
  EXPECT_EQ(sizer.getOptimalBatchSize(), kMinBatchRows);
}

// ---------------------------------------------------------------------------
// Pressure then recovery full cycle
// ---------------------------------------------------------------------------

TEST(AdaptiveBatchSizerTest, FullPressureRecoveryCycle) {
  AdaptiveBatchSizer sizer;
  auto row_type = ROW({"a"}, {BIGINT()});
  sizer.computeOptimalBatchSize(row_type, 16384);
  // 8 bytes/row. 16384 / 8 = 2048.
  EXPECT_EQ(sizer.getCurrentBatchSize(), 2048);

  // Pressure: 2048 → 1024 → 512 → 256 (min).
  sizer.adjustForMemoryPressure(0.90);
  EXPECT_EQ(sizer.getCurrentBatchSize(), 1024);
  sizer.adjustForMemoryPressure(0.90);
  EXPECT_EQ(sizer.getCurrentBatchSize(), 512);
  sizer.adjustForMemoryPressure(0.90);
  EXPECT_EQ(sizer.getCurrentBatchSize(), 256);
  sizer.adjustForMemoryPressure(0.90);
  EXPECT_EQ(sizer.getCurrentBatchSize(), 256);  // Stays at min.

  // Recovery: 256 → 320 → 400 → 500 → 625 → 781 → 976 → 1220→2048
  sizer.adjustForMemoryPressure(0.50);
  EXPECT_EQ(sizer.getCurrentBatchSize(), 320);
  sizer.adjustForMemoryPressure(0.50);
  EXPECT_EQ(sizer.getCurrentBatchSize(), 400);
  sizer.adjustForMemoryPressure(0.50);
  EXPECT_EQ(sizer.getCurrentBatchSize(), 500);
  sizer.adjustForMemoryPressure(0.50);
  EXPECT_EQ(sizer.getCurrentBatchSize(), 625);
  sizer.adjustForMemoryPressure(0.50);
  EXPECT_EQ(sizer.getCurrentBatchSize(), 781);
  sizer.adjustForMemoryPressure(0.50);
  EXPECT_EQ(sizer.getCurrentBatchSize(), 976);
  sizer.adjustForMemoryPressure(0.50);
  EXPECT_EQ(sizer.getCurrentBatchSize(), 1220);
  sizer.adjustForMemoryPressure(0.50);
  EXPECT_EQ(sizer.getCurrentBatchSize(), 1525);
  sizer.adjustForMemoryPressure(0.50);
  EXPECT_EQ(sizer.getCurrentBatchSize(), 1906);
  sizer.adjustForMemoryPressure(0.50);
  EXPECT_EQ(sizer.getCurrentBatchSize(), 2048);  // Caps at optimal.
}

}  // namespace
}  // namespace pangrova::velox
