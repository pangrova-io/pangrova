package org.pangrova.spi.resource.guard;

import org.pangrova.spi.annotation.Experimental;

import java.util.Objects;

/**
 * A point-in-time snapshot of a single query's resource consumption, fed to
 * a {@link RunawayQueryGuard} to decide whether the query has become a runaway.
 *
 * <p>This is the SPI-level equivalent of the implementation-level
 * {@code QueryRuntimeMetrics} in {@code pangrova-resource}.</p>
 *
 * @param queryId      the query identifier (never null or blank)
 * @param cpuTimeMs    cumulative CPU time consumed so far, in milliseconds
 * @param scannedBytes bytes scanned so far
 * @param scannedRows  rows scanned so far
 * @param memoryBytes  memory currently reserved by the query
 */
@Experimental(reason = "G-RES-06: runaway query guard SPI")
public record QueryResourceSnapshot(
        String queryId,
        long cpuTimeMs,
        long scannedBytes,
        long scannedRows,
        long memoryBytes) {

    public QueryResourceSnapshot {
        Objects.requireNonNull(queryId, "queryId must not be null");
        if (queryId.isBlank()) {
            throw new IllegalArgumentException("queryId must not be blank");
        }
    }
}
