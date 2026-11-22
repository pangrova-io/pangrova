package org.pangrova.query.profiling;

import org.pangrova.query.api.audit.QueryAuditRecord;
import org.pangrova.query.audit.QueryAuditService;

import java.util.List;
import java.util.Optional;

/** Publishes query audit totals and measured Native task-attempt profiles. */
public final class QueryProfileService {

    private final QueryAuditService auditService;
    private final OperatorStatsSource operatorStats;

    public QueryProfileService(QueryAuditService auditService) {
        this(auditService, null);
    }

    /** Creates a profile service backed by real Coordinator operator telemetry. */
    public QueryProfileService(QueryAuditService auditService, OperatorStatsSource operatorStats) {
        this.auditService = auditService;
        this.operatorStats = operatorStats;
    }

    /** Retrieves the structured profile for the exact submitting principal. */
    public Optional<QueryProfile> getProfile(String queryId, String principal) {
        if (principal == null || principal.isBlank()) return Optional.empty();
        return auditService.findByQueryId(queryId, principal).map(this::buildProfile);
    }

    private QueryProfile buildProfile(QueryAuditRecord record) {
        List<OperatorStatsSource.Attempt> attempts = operatorStats == null
                ? List.of() : operatorStats.attemptStats(record.queryId());
        var timing = new TimingBreakdown(record.wallTimeMs(), record.cpuTimeMs(),
                record.inputRows(), record.outputRows());
        var memory = new MemoryUsage(record.peakMemoryBytes(), record.spilledBytes(), record.inputBytes(),
                record.outputBytes(), record.shuffleReadBytes(), record.shuffleWriteBytes());
        return new QueryProfile(record.queryId(), record.sql(), record.success(),
                attempts, timing, memory, !attempts.isEmpty(),
                operatorStats == null ? null : operatorStats.executionGraph(record.queryId()));
    }

    /** Structured query profile containing exact attempts and audit totals. */
    public record QueryProfile(
            String queryId,
            String sql,
            boolean success,
            List<OperatorStatsSource.Attempt> attempts,
            TimingBreakdown timing,
            MemoryUsage memory,
            boolean telemetryAvailable,
            OperatorStatsSource.ExecutionGraph executionGraph) {
        public QueryProfile {
            attempts = List.copyOf(attempts);
        }
    }

    /** Timing recorded by the query audit owner. */
    public record TimingBreakdown(
            long totalWallMs,
            long totalCpuMs,
            long inputRows,
            long outputRows) {}

    /** Memory and I/O counters recorded by the query audit owner. */
    public record MemoryUsage(
            long peakMemoryBytes,
            long spilledBytes,
            long inputBytes,
            long outputBytes,
            long shuffleReadBytes,
            long shuffleWriteBytes) {}
}
