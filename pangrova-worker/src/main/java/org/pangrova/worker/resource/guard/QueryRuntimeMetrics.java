package org.pangrova.worker.resource.guard;

/**
 * A point-in-time snapshot of a single query's resource consumption, fed to
 * {@link RunawayQueryGuard} to decide whether the query has become a runaway.
 *
 * @param queryId      the query identifier
 * @param cpuTimeMs    cumulative CPU time consumed so far, in milliseconds
 * @param scannedBytes bytes scanned so far
 * @param scannedRows  rows scanned so far
 * @param memoryBytes  memory currently reserved by the query
 */
public record QueryRuntimeMetrics(
        String queryId,
        long cpuTimeMs,
        long scannedBytes,
        long scannedRows,
        long memoryBytes) {

    public QueryRuntimeMetrics {
        if (queryId == null || queryId.isBlank()) {
            throw new IllegalArgumentException("queryId must not be null or blank");
        }
    }
}
