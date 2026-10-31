package org.pangrova.observability.sdk.logging;

import java.time.Duration;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Logs queries that exceed a configurable duration threshold.
 *
 * <p>When a query completes and its wall-clock duration exceeds the threshold,
 * this component emits a structured WARN-level log entry containing the full
 * query plan, execution statistics, and resource usage for post-hoc analysis.
 */
public final class SlowQueryLog {

    private static final Logger LOG = LoggerFactory.getLogger(SlowQueryLog.class);

    /** Maximum number of SQL characters retained in a slow-query log entry. */
    private static final int MAX_SQL_LENGTH = 2048;

    private final Duration threshold;

    /**
     * Creates a slow query log with the given duration threshold.
     *
     * @param threshold queries exceeding this duration are logged
     */
    public SlowQueryLog(Duration threshold) {
        if (threshold == null || threshold.isNegative() || threshold.isZero()) {
            throw new IllegalArgumentException("threshold must be a positive duration");
        }
        this.threshold = threshold;
    }

    /**
     * Returns the configured threshold.
     */
    public Duration getThreshold() {
        return threshold;
    }

    /**
     * Evaluates whether the query exceeded the threshold and logs it if so.
     *
     * @param entry the completed query execution details
     * @return true if the query was logged as slow, false otherwise
     */
    public boolean evaluate(SlowQueryEntry entry) {
        if (entry.duration().compareTo(threshold) <= 0) {
            return false;
        }
        LOG.warn("Slow query detected: queryId={} duration={}ms threshold={}ms sql=\"{}\" plan=\"{}\" "
                        + "rowsProcessed={} bytesRead={} peakMemoryBytes={} cpuTimeMs={} user={}",
                entry.queryId(),
                entry.duration().toMillis(),
                threshold.toMillis(),
                truncateSql(entry.sql()),
                entry.planSummary(),
                entry.rowsProcessed(),
                entry.bytesRead(),
                entry.peakMemoryBytes(),
                entry.cpuTimeMs(),
                entry.user());
        return true;
    }

    private static String truncateSql(String sql) {
        if (sql == null) {
            return "";
        }
        if (sql.length() <= MAX_SQL_LENGTH) {
            return sql;
        }
        return sql.substring(0, MAX_SQL_LENGTH) + "...[truncated]";
    }

    /**
     * Immutable record capturing all details of a completed query for slow-query evaluation.
     */
    public record SlowQueryEntry(
            String queryId,
            String sql,
            String user,
            Duration duration,
            String planSummary,
            long rowsProcessed,
            long bytesRead,
            long peakMemoryBytes,
            long cpuTimeMs
    ) {
        public SlowQueryEntry {
            if (queryId == null || queryId.isBlank()) {
                throw new IllegalArgumentException("queryId must not be blank");
            }
            if (duration == null) {
                throw new IllegalArgumentException("duration must not be null");
            }
        }
    }
}
