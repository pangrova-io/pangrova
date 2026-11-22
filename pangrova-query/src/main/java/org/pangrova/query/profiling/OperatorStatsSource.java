package org.pangrova.query.profiling;

import java.util.List;

/** Supplies the latest measured counters for each retained Native task attempt. */
public interface OperatorStatsSource {

    /** Exact attempt telemetry for the query, or an empty list when absent. */
    List<Attempt> attemptStats(String queryId);

    /** Coordinates and counters from the latest retained report for one exact attempt. */
    record Attempt(
            int stageId,
            int partitionId,
            long planEpoch,
            int attemptId,
            String taskId,
            String workerId,
            String reportState,
            long reportSequence,
            long elapsedTimeMs,
            long cpuTimeMs,
            long backpressureWaitMs,
            List<Metric> operators) {
        public Attempt {
            operators = List.copyOf(operators);
        }
    }

    /** One operator instance reported by Native, within the named task attempt. */
    record Metric(
            String operatorType,
            String planNodeId,
            long inputRows,
            long outputRows,
            long inputBytes,
            long outputBytes,
            long cpuNanos,
            long wallNanos,
            long peakMemoryBytes,
            long spillBytes) {
    }
}
