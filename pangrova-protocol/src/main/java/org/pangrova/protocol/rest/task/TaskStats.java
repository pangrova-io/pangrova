package org.pangrova.protocol.rest.task;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

/**
 * Task execution statistics aligned with Protobuf TaskStats in
 * pangrova_native_protocol.proto.
 *
 * <p>Field names mirror the Protobuf definition for 1:1 semantic mapping:
 * rowsRead/bytesRead (input), rowsWritten/bytesWritten (output),
 * elapsedTimeMs, cpuTimeMs, memoryBytes, peakMemoryBytes, spillBytes,
 * cacheHitBytes, cacheMissBytes, operatorStats (per-operator breakdown).</p>
 *
 * <p>Transport-layer fields (compressedBytesTransferred, uncompressedBytesTransferred)
 * are REST-only — they track Arrow IPC compression efficiency at the transport
 * transport layer and have no Protobuf counterpart in the native protocol.</p>
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
    @JsonProperty("backpressure_wait_ms") long backpressureWaitMs
) {
    /**
     * Convenience constructor without transport stats, operator stats, and backpressure.
     */
    public TaskStats(long rowsRead, long bytesRead, long rowsWritten,
                     long bytesWritten, long elapsedTimeMs, long cpuTimeMs,
                     long memoryBytes, long peakMemoryBytes, long spillBytes,
                     long cacheHitBytes, long cacheMissBytes) {
        this(rowsRead, bytesRead, rowsWritten, bytesWritten, elapsedTimeMs,
             cpuTimeMs, memoryBytes, peakMemoryBytes, spillBytes,
             cacheHitBytes, cacheMissBytes, 0, 0, List.of(), 0);
    }

    /**
     * Backward-compatible constructor without backpressure.
     */
    public TaskStats(long rowsRead, long bytesRead, long rowsWritten,
                     long bytesWritten, long elapsedTimeMs, long cpuTimeMs,
                     long memoryBytes, long peakMemoryBytes, long spillBytes,
                     long cacheHitBytes, long cacheMissBytes,
                     long compressedBytesTransferred, long uncompressedBytesTransferred,
                     List<OperatorStats> operatorStats) {
        this(rowsRead, bytesRead, rowsWritten, bytesWritten, elapsedTimeMs,
             cpuTimeMs, memoryBytes, peakMemoryBytes, spillBytes,
             cacheHitBytes, cacheMissBytes, compressedBytesTransferred,
             uncompressedBytesTransferred, operatorStats, 0);
    }
}
