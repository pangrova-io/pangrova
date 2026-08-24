package org.pangrova.worker.task;

import org.pangrova.protocol.rest.task.OperatorStats;
import org.pangrova.worker.jni.NativeRuntime;
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
 * a {@code pangrova.protocol.v1.TaskStats} protobuf that contains a
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
     * Collects per-operator stats from the native engine for the given
     * executor handle. Returns an empty list if the handle is invalid,
     * the native runtime is unavailable, or parsing fails.
     *
     * @param executorHandle the native task executor handle
     * @return decoded per-operator stats, never null
     */
    static List<OperatorStats> collect(long executorHandle) {
        if (executorHandle == 0) {
            return List.of();
        }
        try {
            byte[] rawStats = NativeRuntime.nativeGetTaskStats(executorHandle);
            return decodeOperatorStats(rawStats);
        } catch (Exception e) {
            LOG.debug("event=native_operator_stats_collection_failed handle={} error={}",
                    executorHandle, e.getMessage());
            return List.of();
        }
    }

    /**
     * Decodes the serialized {@code pangrova.protocol.v1.TaskStats} protobuf
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
            org.pangrova.protocol.v1.TaskStats protoStats =
                    org.pangrova.protocol.v1.TaskStats.parseFrom(rawTaskStats);
            List<org.pangrova.protocol.v1.OperatorStats> protoOps =
                    protoStats.getOperatorStatsList();
            if (protoOps.isEmpty()) {
                return List.of();
            }
            List<OperatorStats> result = new ArrayList<>(protoOps.size());
            for (org.pangrova.protocol.v1.OperatorStats pop : protoOps) {
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

    /**
     * Emits per-operator stats as structured log entries for observability.
     * Mirrors the structured logging format in
     * {@link org.pangrova.observability.metrics.NativeMetricsPublisher#publishOperatorStats}.
     *
     * @param taskId        the task identifier for log correlation
     * @param operatorStats the per-operator stats to log
     */
    static void logOperatorStats(String taskId, List<OperatorStats> operatorStats) {
        if (operatorStats == null || operatorStats.isEmpty()) {
            return;
        }
        for (OperatorStats stats : operatorStats) {
            LOG.info("event=native_operator_stats taskId={} operator_type={} plan_node_id={} "
                            + "input_rows={} output_rows={} input_bytes={} output_bytes={} "
                            + "cpu_nanos={} wall_nanos={} memory_bytes={} peak_memory_bytes={} "
                            + "spill_bytes={}",
                    taskId, stats.operatorType(), stats.planNodeId(),
                    stats.inputRows(), stats.outputRows(),
                    stats.inputBytes(), stats.outputBytes(),
                    stats.cpuNanos(), stats.wallNanos(),
                    stats.memoryBytes(), stats.peakMemoryBytes(), stats.spillBytes());
        }
    }
}
