package org.pangrova.worker.task;

import org.pangrova.protocol.rest.task.OperatorStats;
import org.pangrova.protocol.rest.task.TaskStats;
import org.pangrova.protocol.v2.NativeTaskStats;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * Collects per-operator execution statistics from the native engine
 * via JNI and decodes the protobuf response into REST-layer DTOs.
 *
 * <p>G-OBS-06 fix: this class bridges the gap between the existing
 * {@code nativeGetTaskStats} JNI entry point (which was declared but never
 * called) and the Java-side observability layer. The native side serializes
 * a canonical v2 {@code NativeTaskStats} protobuf that contains a
 * {@code repeated OperatorStats} field; this collector decodes that field
 * into {@link OperatorStats} DTOs suitable for REST responses and metrics
 * emission.</p>
 *
 * <p>All methods are safe to call from any thread and never throw.
 * On failure (JNI unavailable, malformed proto, handle already released),
 * an empty list is returned.</p>
 */
final class NativeOperatorStatsCollector {

    private static final Logger LOG = LoggerFactory.getLogger(NativeOperatorStatsCollector.class);

    private NativeOperatorStatsCollector() {}

    /**
     * Decodes the serialized canonical v2 {@code NativeTaskStats} protobuf
     * and extracts the {@code repeated OperatorStats} field.
     *
     * <p>Uses the generated protobuf classes for safe, forward-compatible
     * decoding. Unknown fields are silently ignored per proto3 semantics.</p>
     */
    static List<OperatorStats> decodeOperatorStats(byte[] rawTaskStats) {
        if (rawTaskStats == null || rawTaskStats.length == 0) {
            return List.of();
        }
        try {
            org.pangrova.protocol.v2.NativeTaskStats protoStats =
                    org.pangrova.protocol.v2.NativeTaskStats.parseFrom(rawTaskStats);
            List<org.pangrova.protocol.v2.OperatorStats> protoOps =
                    protoStats.getOperatorStatsList();
            if (protoOps.isEmpty()) {
                return List.of();
            }
            List<OperatorStats> result = new ArrayList<>(protoOps.size());
            for (org.pangrova.protocol.v2.OperatorStats pop : protoOps) {
                result.add(new OperatorStats(
                        pop.getOperatorType(),
                        pop.getInputRows(),
                        pop.getOutputRows(),
                        pop.getInputBytes(),
                        pop.getOutputBytes(),
                        pop.getCpuNanos(),
                        pop.getWallNanos(),
                        pop.getPlanNodeId(),
                        pop.getMemoryBytes(),
                        pop.getPeakMemoryBytes(),
                        pop.getSpillBytes()));
            }
            if (LOG.isDebugEnabled()) {
                LOG.debug("event=native_operator_stats_decoded operatorCount={}", result.size());
            }
            return List.copyOf(result);
        } catch (Exception e) {
            LOG.debug("event=native_operator_stats_decode_failed error={}", e.getMessage());
            return List.of();
        }
    }

    /** Projects already captured Native statistics into the report summary. */
    static TaskStats toTaskStats(NativeTaskStats protoStats) {
        return new TaskStats(
                protoStats.getRowsRead(),
                protoStats.getBytesRead(),
                protoStats.getRowsWritten(),
                protoStats.getBytesWritten(),
                protoStats.getElapsedTimeMs(),
                protoStats.getCpuTimeMs(),
                protoStats.getMemoryBytes(),
                protoStats.getPeakMemoryBytes(),
                protoStats.getSpillBytes(),
                protoStats.getCacheHitBytes(),
                protoStats.getCacheMissBytes(),
                0,
                0,
                mapOperatorStats(protoStats.getOperatorStatsList()),
                protoStats.getBackpressureWaitMs(),
                protoStats.getCacheHitCount(),
                protoStats.getCacheMissCount(),
                protoStats.getCacheEvictionCount(),
                protoStats.getCacheAdmissionRejectionCount());
    }

    private static List<OperatorStats> mapOperatorStats(
            List<org.pangrova.protocol.v2.OperatorStats> protoOps) {
        if (protoOps.isEmpty()) {
            return List.of();
        }
        List<OperatorStats> result = new ArrayList<>(protoOps.size());
        for (org.pangrova.protocol.v2.OperatorStats pop : protoOps) {
            result.add(new OperatorStats(
                    pop.getOperatorType(),
                    pop.getInputRows(),
                    pop.getOutputRows(),
                    pop.getInputBytes(),
                    pop.getOutputBytes(),
                    pop.getCpuNanos(),
                    pop.getWallNanos(),
                    pop.getPlanNodeId(),
                    pop.getMemoryBytes(),
                    pop.getPeakMemoryBytes(),
                    pop.getSpillBytes()));
        }
        return List.copyOf(result);
    }

}
