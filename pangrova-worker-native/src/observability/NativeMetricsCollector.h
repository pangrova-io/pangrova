#pragma once

#include <atomic>
#include <cstdint>

namespace pangrova::observability {

struct TaskStatsSnapshot {
    int64_t cpu_time_ns;
    int64_t wall_time_ns;
    int64_t peak_memory_bytes;
    int64_t rows_read;
    int64_t bytes_read;
    // Connector-specific metrics for Velox native path observability.
    int64_t merge_reader_input_rows;
    int64_t merge_reader_output_rows;
    int64_t deletion_vector_filtered_rows;
    int64_t schema_evolution_columns_mapped;
    int64_t partition_columns_injected;
    int64_t predicate_pushdown_row_groups_skipped;
    // F-NATIVE-EXEC-08: Queue depth and capacity metrics for observability.
    int64_t output_queue_depth;
    int64_t output_queue_capacity;
    int64_t shuffle_queue_depth;
    int64_t shuffle_queue_capacity;
};

/**
 * Lock-free metrics accumulator using atomic operations.
 * Thread-safe for concurrent updates from multiple operator threads.
 */
class NativeMetricsCollector {
public:
    void addCpuTime(int64_t ns);
    void addWallTime(int64_t ns);
    void addRows(int64_t rows);
    void addBytes(int64_t bytes);
    void updatePeakMemory(int64_t bytes);

    // Connector-specific metric accumulators.
    void addMergeReaderInputRows(int64_t rows);
    void addMergeReaderOutputRows(int64_t rows);
    void addDeletionVectorFilteredRows(int64_t rows);
    void addSchemaEvolutionColumnsMapped(int64_t columns);
    void addPartitionColumnsInjected(int64_t columns);
    void addPredicatePushdownRowGroupsSkipped(int64_t groups);

    TaskStatsSnapshot snapshot() const;
    void reset();

private:
    std::atomic<int64_t> cpu_time_ns_{0};
    std::atomic<int64_t> wall_time_ns_{0};
    std::atomic<int64_t> peak_memory_bytes_{0};
    std::atomic<int64_t> rows_read_{0};
    std::atomic<int64_t> bytes_read_{0};
    // Connector-specific counters.
    std::atomic<int64_t> merge_reader_input_rows_{0};
    std::atomic<int64_t> merge_reader_output_rows_{0};
    std::atomic<int64_t> deletion_vector_filtered_rows_{0};
    std::atomic<int64_t> schema_evolution_columns_mapped_{0};
    std::atomic<int64_t> partition_columns_injected_{0};
    std::atomic<int64_t> predicate_pushdown_row_groups_skipped_{0};
};

} // namespace pangrova::observability
