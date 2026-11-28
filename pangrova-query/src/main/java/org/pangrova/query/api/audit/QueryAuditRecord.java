package org.pangrova.query.api.audit;

import org.pangrova.common.api.annotation.Experimental;

import java.time.Instant;

/**
 * Immutable record capturing resource usage and outcome of a completed query.
 * Used for audit trail persistence and post-hoc analysis.
 */
@Experimental
public record QueryAuditRecord(
        String queryId,
        String userId,
        String sql,
        String tenantId,
        int stageCount,
        int taskCount,
        long cpuTimeMs,
        long wallTimeMs,
        long peakMemoryBytes,
        long spilledBytes,
        long inputRows,
        long outputRows,
        long inputBytes,
        long outputBytes,
        long shuffleReadBytes,
        long shuffleWriteBytes,
        Instant completedAt,
        boolean success
) {
    /** Creates the terminal record when the Coordinator measures wall time. */
    public static QueryAuditRecord wallTimeOnly(String queryId, String principal, String sql, String tenantId,
            long wallTimeMs, Instant completedAt, boolean success) {
        return new QueryAuditRecord(queryId, java.util.Objects.requireNonNull(principal, "principal"), sql,
                tenantId, 0, 0, 0, wallTimeMs, 0, 0, 0, 0, 0, 0, 0, 0, completedAt, success);
    }

    /**
     * Backward-compatible constructor without sql, tenantId, stageCount, taskCount.
     */
    public QueryAuditRecord(String queryId, String userId,
                            long cpuTimeMs, long wallTimeMs, long peakMemoryBytes,
                            long spilledBytes, long inputRows, long outputRows,
                            long inputBytes, long outputBytes,
                            long shuffleReadBytes, long shuffleWriteBytes,
                            Instant completedAt, boolean success) {
        this(queryId, userId, null, null, 0, 0,
             cpuTimeMs, wallTimeMs, peakMemoryBytes, spilledBytes,
             inputRows, outputRows, inputBytes, outputBytes,
             shuffleReadBytes, shuffleWriteBytes, completedAt, success);
    }
}
