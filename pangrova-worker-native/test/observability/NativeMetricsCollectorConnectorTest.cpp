// Tests for NativeMetricsCollector connector-specific metrics.
// Validates that new connector metrics (merge reader, DV, schema evolution,
// partition injection, predicate pushdown) accumulate correctly and appear
// in the snapshot.
#include <gtest/gtest.h>

#include <thread>
#include <vector>

#include "observability/NativeMetricsCollector.h"

namespace pangrova::observability::test {
namespace {

TEST(NativeMetricsCollectorConnectorTest, InitialConnectorMetricsAreZero) {
  NativeMetricsCollector collector;
  auto snap = collector.snapshot();
  EXPECT_EQ(snap.merge_reader_input_rows, 0);
  EXPECT_EQ(snap.merge_reader_output_rows, 0);
  EXPECT_EQ(snap.deletion_vector_filtered_rows, 0);
  EXPECT_EQ(snap.schema_evolution_columns_mapped, 0);
  EXPECT_EQ(snap.partition_columns_injected, 0);
  EXPECT_EQ(snap.predicate_pushdown_row_groups_skipped, 0);
}

TEST(NativeMetricsCollectorConnectorTest, MergeReaderMetricsAccumulate) {
  NativeMetricsCollector collector;
  collector.addMergeReaderInputRows(1000);
  collector.addMergeReaderInputRows(500);
  collector.addMergeReaderOutputRows(800);
  auto snap = collector.snapshot();
  EXPECT_EQ(snap.merge_reader_input_rows, 1500);
  EXPECT_EQ(snap.merge_reader_output_rows, 800);
}

TEST(NativeMetricsCollectorConnectorTest, DeletionVectorFilteredRows) {
  NativeMetricsCollector collector;
  collector.addDeletionVectorFilteredRows(42);
  collector.addDeletionVectorFilteredRows(8);
  auto snap = collector.snapshot();
  EXPECT_EQ(snap.deletion_vector_filtered_rows, 50);
}

TEST(NativeMetricsCollectorConnectorTest, SchemaEvolutionColumnsMapped) {
  NativeMetricsCollector collector;
  collector.addSchemaEvolutionColumnsMapped(3);
  auto snap = collector.snapshot();
  EXPECT_EQ(snap.schema_evolution_columns_mapped, 3);
}

TEST(NativeMetricsCollectorConnectorTest, PartitionColumnsInjected) {
  NativeMetricsCollector collector;
  collector.addPartitionColumnsInjected(2);
  collector.addPartitionColumnsInjected(1);
  auto snap = collector.snapshot();
  EXPECT_EQ(snap.partition_columns_injected, 3);
}

TEST(NativeMetricsCollectorConnectorTest, PredicatePushdownRowGroupsSkipped) {
  NativeMetricsCollector collector;
  collector.addPredicatePushdownRowGroupsSkipped(10);
  auto snap = collector.snapshot();
  EXPECT_EQ(snap.predicate_pushdown_row_groups_skipped, 10);
}

TEST(NativeMetricsCollectorConnectorTest, ResetClearsConnectorMetrics) {
  NativeMetricsCollector collector;
  collector.addMergeReaderInputRows(100);
  collector.addMergeReaderOutputRows(80);
  collector.addDeletionVectorFilteredRows(20);
  collector.addSchemaEvolutionColumnsMapped(5);
  collector.addPartitionColumnsInjected(3);
  collector.addPredicatePushdownRowGroupsSkipped(7);
  collector.reset();
  auto snap = collector.snapshot();
  EXPECT_EQ(snap.merge_reader_input_rows, 0);
  EXPECT_EQ(snap.merge_reader_output_rows, 0);
  EXPECT_EQ(snap.deletion_vector_filtered_rows, 0);
  EXPECT_EQ(snap.schema_evolution_columns_mapped, 0);
  EXPECT_EQ(snap.partition_columns_injected, 0);
  EXPECT_EQ(snap.predicate_pushdown_row_groups_skipped, 0);
}

TEST(NativeMetricsCollectorConnectorTest, ConcurrentConnectorMetricUpdates) {
  NativeMetricsCollector collector;
  constexpr int kThreads = 4;
  constexpr int kIterations = 1000;

  std::vector<std::thread> threads;
  for (int t = 0; t < kThreads; ++t) {
    threads.emplace_back([&collector]() {
      for (int i = 0; i < kIterations; ++i) {
        collector.addMergeReaderInputRows(1);
        collector.addDeletionVectorFilteredRows(1);
      }
    });
  }
  for (auto& t : threads) {
    t.join();
  }

  auto snap = collector.snapshot();
  EXPECT_EQ(snap.merge_reader_input_rows, kThreads * kIterations);
  EXPECT_EQ(snap.deletion_vector_filtered_rows, kThreads * kIterations);
}

TEST(NativeMetricsCollectorConnectorTest, ConnectorMetricsIndependentOfBase) {
  NativeMetricsCollector collector;
  collector.addRows(500);
  collector.addBytes(1024);
  collector.addMergeReaderInputRows(100);
  auto snap = collector.snapshot();
  EXPECT_EQ(snap.rows_read, 500);
  EXPECT_EQ(snap.bytes_read, 1024);
  EXPECT_EQ(snap.merge_reader_input_rows, 100);
  // Base metrics unaffected by connector metrics.
  EXPECT_EQ(snap.merge_reader_output_rows, 0);
}

TEST(NativeMetricsCollectorConnectorTest, CumulativeCopyObservationsAreNotDoubleCounted) {
  NativeMetricsCollector collector;
  collector.observeColumnarConversionCopiedBytes(24);
  collector.observeColumnarConversionCopiedBytes(24);
  collector.observeColumnarConversionCopiedBytes(56);
  EXPECT_EQ(collector.snapshot().columnar_conversion_copied_bytes, 56);
  collector.reset();
  EXPECT_EQ(collector.snapshot().columnar_conversion_copied_bytes, 0);
}

TEST(NativeMetricsCollectorConnectorTest, ConcurrentCumulativeCopiesKeepTheActualMaximum) {
  NativeMetricsCollector collector;
  std::vector<std::thread> threads;
  for (uint64_t worker = 0; worker < 4; ++worker) {
    threads.emplace_back([&collector, worker] {
      for (uint64_t count = 1; count <= 100; ++count) {
        collector.observeColumnarConversionCopiedBytes(worker * 100 + count);
      }
    });
  }
  for (auto& thread : threads) thread.join();
  EXPECT_EQ(collector.snapshot().columnar_conversion_copied_bytes, 400);
}

}  // namespace
}  // namespace pangrova::observability::test
