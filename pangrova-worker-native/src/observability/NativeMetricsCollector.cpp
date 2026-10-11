#include "observability/NativeMetricsCollector.h"

#include <algorithm>
#include <limits>
#include <stdexcept>
#include <string>

#include "observability/ColumnarReadMetrics.h"
#include "velox/exec/TaskStats.h"

namespace pangrova::observability {

void NativeMetricsCollector::addCpuTime(int64_t ns) {
    cpu_time_ns_.fetch_add(ns, std::memory_order_relaxed);
}

void NativeMetricsCollector::addWallTime(int64_t ns) {
    wall_time_ns_.fetch_add(ns, std::memory_order_relaxed);
}

void NativeMetricsCollector::addRows(int64_t rows) {
    rows_read_.fetch_add(rows, std::memory_order_relaxed);
}

void NativeMetricsCollector::addBytes(int64_t bytes) {
    bytes_read_.fetch_add(bytes, std::memory_order_relaxed);
}

void NativeMetricsCollector::updatePeakMemory(int64_t bytes) {
    int64_t current = peak_memory_bytes_.load(std::memory_order_relaxed);
    while (bytes > current) {
        if (peak_memory_bytes_.compare_exchange_weak(current, bytes,
                std::memory_order_relaxed)) {
            break;
        }
    }
}

void NativeMetricsCollector::observeColumnarConversionCopiedBytes(
    uint64_t cumulative_bytes) {
    auto current = columnar_conversion_copied_bytes_.load(std::memory_order_relaxed);
    while (cumulative_bytes > current &&
           !columnar_conversion_copied_bytes_.compare_exchange_weak(
               current, cumulative_bytes, std::memory_order_relaxed)) {
    }
}

TaskStatsSnapshot NativeMetricsCollector::snapshot() const {
    return TaskStatsSnapshot{
        cpu_time_ns_.load(std::memory_order_relaxed),
        wall_time_ns_.load(std::memory_order_relaxed),
        peak_memory_bytes_.load(std::memory_order_relaxed),
        rows_read_.load(std::memory_order_relaxed),
        bytes_read_.load(std::memory_order_relaxed),
        merge_reader_input_rows_.load(std::memory_order_relaxed),
        merge_reader_output_rows_.load(std::memory_order_relaxed),
        deletion_vector_filtered_rows_.load(std::memory_order_relaxed),
        schema_evolution_columns_mapped_.load(std::memory_order_relaxed),
        partition_columns_injected_.load(std::memory_order_relaxed),
        predicate_pushdown_row_groups_skipped_.load(std::memory_order_relaxed),
        // F-NATIVE-EXEC-08: Queue metrics are populated by TaskExecutor at
        // snapshot time, not accumulated here. Default to 0.
        0, 0, 0, 0,
        columnar_conversion_copied_bytes_.load(std::memory_order_relaxed) +
            columnar_read_copied_bytes_.load(std::memory_order_relaxed)
    };
}

void NativeMetricsCollector::reset() {
    // P1-OBS-7: Non-atomic reset of 11 fields.
    // Each individual store is atomic (relaxed), but the aggregate reset is
    // NOT atomic across all fields — a concurrent snapshot() call may observe
    // a mix of pre-reset and post-reset values. This is acceptable because:
    // 1. reset() is called only during task cleanup (single-threaded context).
    // 2. snapshot() consumers tolerate approximate values (monitoring, not billing).
    // If strict consistency is needed, use a generation counter or double-buffering.
    cpu_time_ns_.store(0, std::memory_order_relaxed);
    wall_time_ns_.store(0, std::memory_order_relaxed);
    peak_memory_bytes_.store(0, std::memory_order_relaxed);
    rows_read_.store(0, std::memory_order_relaxed);
    bytes_read_.store(0, std::memory_order_relaxed);
    columnar_conversion_copied_bytes_.store(0, std::memory_order_relaxed);
    columnar_read_copied_bytes_.store(0, std::memory_order_relaxed);
    merge_reader_input_rows_.store(0, std::memory_order_relaxed);
    merge_reader_output_rows_.store(0, std::memory_order_relaxed);
    deletion_vector_filtered_rows_.store(0, std::memory_order_relaxed);
    schema_evolution_columns_mapped_.store(0, std::memory_order_relaxed);
    partition_columns_injected_.store(0, std::memory_order_relaxed);
    predicate_pushdown_row_groups_skipped_.store(0, std::memory_order_relaxed);
}

void NativeMetricsCollector::observeColumnarReadTaskStats(
    const facebook::velox::exec::TaskStats& stats) {
    uint64_t total = 0;
    for (const auto& pipeline : stats.pipelineStats) {
        for (const auto& op : pipeline.operatorStats) {
            const auto metric = op.runtimeStats.find(std::string(kColumnarReadCopiedBytes));
            if (metric == op.runtimeStats.end()) continue;
            if (metric->second.unit != facebook::velox::RuntimeCounter::Unit::kBytes ||
                metric->second.sum < 0) {
                throw std::runtime_error("Invalid actual columnar read copy metric");
            }
            const auto bytes = static_cast<uint64_t>(metric->second.sum);
            if (bytes > std::numeric_limits<uint64_t>::max() - total) {
                throw std::overflow_error("Columnar read copy metric overflow");
            }
            total += bytes;
        }
    }
    auto current = columnar_read_copied_bytes_.load(std::memory_order_relaxed);
    while (total > current && !columnar_read_copied_bytes_.compare_exchange_weak(
        current, total, std::memory_order_relaxed)) {}
}

void NativeMetricsCollector::addMergeReaderInputRows(int64_t rows) {
    merge_reader_input_rows_.fetch_add(rows, std::memory_order_relaxed);
}

void NativeMetricsCollector::addMergeReaderOutputRows(int64_t rows) {
    merge_reader_output_rows_.fetch_add(rows, std::memory_order_relaxed);
}

void NativeMetricsCollector::addDeletionVectorFilteredRows(int64_t rows) {
    deletion_vector_filtered_rows_.fetch_add(rows, std::memory_order_relaxed);
}

void NativeMetricsCollector::addSchemaEvolutionColumnsMapped(int64_t columns) {
    schema_evolution_columns_mapped_.fetch_add(columns, std::memory_order_relaxed);
}

void NativeMetricsCollector::addPartitionColumnsInjected(int64_t columns) {
    partition_columns_injected_.fetch_add(columns, std::memory_order_relaxed);
}

void NativeMetricsCollector::addPredicatePushdownRowGroupsSkipped(int64_t groups) {
    predicate_pushdown_row_groups_skipped_.fetch_add(groups, std::memory_order_relaxed);
}

} // namespace pangrova::observability
