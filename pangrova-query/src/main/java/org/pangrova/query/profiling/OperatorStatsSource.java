package org.pangrova.query.profiling;

import java.util.List;

/**
 * Supplies per-operator execution telemetry for one query.
 *
 * <p>Implemented by the Coordinator, which owns task status reports. Returning an
 * empty list means no telemetry was retained for that query; the profile must then
 * report {@code telemetryAvailable=false} rather than inventing a tree.</p>
 */
public interface OperatorStatsSource {

    /** Aggregated per-plan-node metrics for the query, or an empty list when absent. */
    List<Metric> operatorStats(String queryId);

    /**
     * One operator's aggregated counters. Identity is the plan node, so retries of
     * the same task contribute once per node rather than once per attempt.
     */
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
