package org.pangrova.observability.api.event;

import org.pangrova.common.api.annotation.Experimental;

/**
 * Aggregate per-query execution metrics carried by {@link QueryCompletedEvent}.
 *
 * <p>Fields not yet threaded through the execution path are reported as {@code 0};
 * {@code wallTimeMillis} is always populated. The shape is intentionally additive so
 * CPU/IO/shuffle counters can be filled in without breaking listeners.</p>
 *
 * @param wallTimeMillis end-to-end wall time from admission to terminal state
 * @param cpuTimeMillis  aggregate CPU time across stages ({@code 0} if unavailable)
 * @param totalRows      total output rows ({@code 0} if unavailable)
 * @param totalBytes     total processed bytes ({@code 0} if unavailable)
 */
@Experimental
public record QueryStatistics(
        long wallTimeMillis,
        long cpuTimeMillis,
        long totalRows,
        long totalBytes) {

    /**
     * Builds a statistics payload with only wall time populated — the common case
     * until CPU/row/byte counters are threaded through the dispatch path.
     *
     * @param wallTimeMillis end-to-end wall time
     * @return statistics with all other counters zero
     */
    public static QueryStatistics wallTimeOnly(long wallTimeMillis) {
        return new QueryStatistics(wallTimeMillis, 0, 0, 0);
    }
}
