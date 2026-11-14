package org.pangrova.worker.task;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Per-task metrics snapshot reported by workers.
 *
 * <p>Captures CPU time, wall time, memory peak, rows processed, bytes read/written,
 * and spill bytes for each task execution. Immutable snapshot taken at task
 * completion or on periodic collection.</p>
 *
 * @param taskId         the task identifier
 * @param queryId        the parent query identifier
 * @param cpuTimeNanos   CPU time consumed by this task in nanoseconds
 * @param wallTimeNanos  wall clock time from start to completion in nanoseconds
 * @param memoryPeakBytes peak memory usage during task execution
 * @param rowsProcessed  total number of rows processed (input + output)
 * @param bytesRead      total bytes read from storage
 * @param bytesWritten   total bytes written to storage or shuffle
 * @param spillBytes     total bytes spilled to disk
 */
public record TaskMetricsSnapshot(
        @JsonProperty("task_id") String taskId,
        @JsonProperty("query_id") String queryId,
        @JsonProperty("cpu_time_nanos") long cpuTimeNanos,
        @JsonProperty("wall_time_nanos") long wallTimeNanos,
        @JsonProperty("memory_peak_bytes") long memoryPeakBytes,
        @JsonProperty("rows_processed") long rowsProcessed,
        @JsonProperty("bytes_read") long bytesRead,
        @JsonProperty("bytes_written") long bytesWritten,
        @JsonProperty("spill_bytes") long spillBytes) {

    /** Nanoseconds per millisecond, for time-unit conversion. */
    private static final long NANOS_PER_MILLI = 1_000_000;

    public TaskMetricsSnapshot {
        if (taskId == null || taskId.isBlank()) {
            throw new IllegalArgumentException("taskId must not be blank");
        }
        if (queryId == null || queryId.isBlank()) {
            throw new IllegalArgumentException("queryId must not be blank");
        }
        if (cpuTimeNanos < 0) {
            throw new IllegalArgumentException("cpuTimeNanos must be non-negative: " + cpuTimeNanos);
        }
        if (wallTimeNanos < 0) {
            throw new IllegalArgumentException("wallTimeNanos must be non-negative: " + wallTimeNanos);
        }
    }

    /**
     * Returns wall time in milliseconds for human-readable reporting.
     */
    public long wallTimeMs() {
        return wallTimeNanos / NANOS_PER_MILLI;
    }

    /**
     * Returns CPU time in milliseconds for human-readable reporting.
     */
    public long cpuTimeMs() {
        return cpuTimeNanos / NANOS_PER_MILLI;
    }
}
