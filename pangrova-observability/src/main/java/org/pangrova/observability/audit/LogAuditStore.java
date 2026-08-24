package org.pangrova.observability.audit;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.Optional;

/**
 * AuditStore that persists QueryAuditRecords to structured log output
 * and retains them in memory for profile lookups.
 * <p>
 * Uses Caffeine (W-TinyLFU) for bounded in-memory caching with O(1) amortized
 * eviction — the industry-standard approach for bounded concurrent caches.
 * Default backend when no external audit database is configured.
 */
public final class LogAuditStore implements QueryAuditService.AuditStore {

    private static final Logger AUDIT_LOG = LoggerFactory.getLogger("pangrova.audit.query");
    private static final int DEFAULT_MAX_ENTRIES = 10_000;
    private static final int MAX_SQL_LENGTH = 200;
    private static final long SECONDS_PER_DAY = 24L * 60 * 60;
    private static final LogAuditStore SHARED_INSTANCE = new LogAuditStore(DEFAULT_MAX_ENTRIES);

    private final Cache<String, QueryAuditRecord> cache;

    /**
     * Creates an in-memory store retaining up to {@code maxEntries} audit records.
     */
    public LogAuditStore(int maxEntries) {
        if (maxEntries <= 0) {
            throw new IllegalArgumentException("maxEntries must be positive, got: " + maxEntries);
        }
        this.cache = Caffeine.newBuilder()
                .maximumSize(maxEntries)
                .build();
    }

    /** Creates a store with the default capacity of {@value #DEFAULT_MAX_ENTRIES} entries. */
    public LogAuditStore() {
        this(DEFAULT_MAX_ENTRIES);
    }

    /**
     * Returns the shared singleton instance used across the coordinator.
     * Ensures the same store is used for both audit persistence and profile lookups.
     */
    public static LogAuditStore shared() {
        return SHARED_INSTANCE;
    }

    @Override
    public void save(QueryAuditRecord record) {
        AUDIT_LOG.info("query={} user={} tenant={} sql={} stages={} tasks={} "
                        + "cpu={}ms wall={}ms peak_memory={} spilled={} "
                        + "input_rows={} output_rows={} success={}",
                record.queryId(), record.userId(), record.tenantId(),
                truncateSql(record.sql()), record.stageCount(), record.taskCount(),
                record.cpuTimeMs(), record.wallTimeMs(),
                record.peakMemoryBytes(), record.spilledBytes(),
                record.inputRows(), record.outputRows(), record.success());
        cache.put(record.queryId(), record);
    }

    private static String truncateSql(String sql) {
        if (sql == null) {
            return "null";
        }
        return sql.length() > MAX_SQL_LENGTH ? sql.substring(0, MAX_SQL_LENGTH) + "..." : sql;
    }

    @Override
    public Optional<QueryAuditRecord> findById(String queryId) {
        return Optional.ofNullable(cache.getIfPresent(queryId));
    }

    @Override
    public long deleteOlderThan(int retentionDays) {
        Instant cutoff = Instant.now().minusSeconds((long) retentionDays * SECONDS_PER_DAY);
        long[] count = {0};
        cache.asMap().entrySet().removeIf(entry -> {
            Instant completedAt = entry.getValue().completedAt();
            if (completedAt != null && completedAt.isBefore(cutoff)) {
                count[0]++;
                return true;
            }
            return false;
        });
        return count[0];
    }
}
