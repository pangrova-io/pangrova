package org.pangrova.protocol.rest.task;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.pangrova.protocol.v2.NativeTaskStats;

import java.util.List;
import java.util.Objects;

/**
 * Task execution statistics mapped from the canonical Protobuf task status.
 *
 * <p>Field names mirror the Protobuf definition for 1:1 semantic mapping:
 * rowsRead/bytesRead (input), rowsWritten/bytesWritten (output),
 * elapsedTimeMs, cpuTimeMs, memoryBytes, peakMemoryBytes, spillBytes,
 * cacheHitBytes, cacheMissBytes, operatorStats (per-operator breakdown).</p>
 *
 * <p>Transport-layer fields (compressedBytesTransferred, uncompressedBytesTransferred)
 * are REST-only — they track Arrow IPC compression efficiency at the transport
 * layer and have no Protobuf counterpart in the native protocol.</p>
 */
public record TaskStats(
    @JsonProperty("rows_read") long rowsRead,
    @JsonProperty("bytes_read") long bytesRead,
    @JsonProperty("rows_written") long rowsWritten,
    @JsonProperty("bytes_written") long bytesWritten,
    @JsonProperty("elapsed_time_ms") long elapsedTimeMs,
    @JsonProperty("cpu_time_ms") long cpuTimeMs,
    @JsonProperty("memory_bytes") long memoryBytes,
    @JsonProperty("peak_memory_bytes") long peakMemoryBytes,
    @JsonProperty("spill_bytes") long spillBytes,
    @JsonProperty("cache_hit_bytes") long cacheHitBytes,
    @JsonProperty("cache_miss_bytes") long cacheMissBytes,
    @JsonProperty("compressed_bytes_transferred") long compressedBytesTransferred,
    @JsonProperty("uncompressed_bytes_transferred") long uncompressedBytesTransferred,
    @JsonProperty("operator_stats") List<OperatorStats> operatorStats,
    @JsonProperty("backpressure_wait_ms") long backpressureWaitMs,
    @JsonProperty("cache_hit_count") long cacheHitCount,
    @JsonProperty("cache_miss_count") long cacheMissCount,
    @JsonProperty("cache_eviction_count") long cacheEvictionCount,
    @JsonProperty("cache_admission_rejection_count") long cacheAdmissionRejectionCount
) {
    public TaskStats {
        operatorStats = List.copyOf(Objects.requireNonNull(
                operatorStats, "operatorStats must not be null"));
    }

    /** Projects canonical worker statistics for diagnostics. */
    public static TaskStats fromNative(NativeTaskStats stats) {
        var operators = stats.getOperatorStatsList().stream().map(op -> new OperatorStats(
                op.getOperatorType(), op.getInputRows(), op.getOutputRows(), op.getInputBytes(),
                op.getOutputBytes(), op.getCpuNanos(), op.getWallNanos(), op.getPlanNodeId(),
                op.getMemoryBytes(), op.getPeakMemoryBytes(), op.getSpillBytes())).toList();
        return new TaskStats(stats.getRowsRead(), stats.getBytesRead(), stats.getRowsWritten(),
                stats.getBytesWritten(), stats.getElapsedTimeMs(), stats.getCpuTimeMs(), stats.getMemoryBytes(),
                stats.getPeakMemoryBytes(), stats.getSpillBytes(), stats.getCacheHitBytes(), stats.getCacheMissBytes(),
                0, 0, operators, stats.getBackpressureWaitMs(), stats.getCacheHitCount(), stats.getCacheMissCount(),
                stats.getCacheEvictionCount(), stats.getCacheAdmissionRejectionCount());
    }
}
