package org.pangrova.planner.advisor;

import javax.annotation.concurrent.ThreadSafe;
import java.time.Instant;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.List;
import java.util.Set;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Records query patterns for workload analysis.
 *
 * <p>During query compilation, the planner extracts structural patterns
 * (filter columns, join columns, sort columns, group-by columns) from
 * the optimized plan. These patterns are recorded here for consumption by
 * {@code DataLayoutAdvisor} which recommends partition keys, sort orders,
 * and compaction strategies based on observed workload.</p>
 *
 * <p>Uses a bounded ring buffer to keep memory usage constant.</p>
 */
@ThreadSafe
public final class QueryPatternRecorder {

    private static final Logger LOG = LoggerFactory.getLogger(QueryPatternRecorder.class);

    private final int maxPatterns;
    private final Deque<QueryPattern> buffer;
    private final ReadWriteLock lock = new ReentrantReadWriteLock();

    /**
     * Creates a recorder bounded to the given number of retained patterns.
     *
     * @param maxPatterns maximum number of patterns to keep
     */
    public QueryPatternRecorder(int maxPatterns) {
        if (maxPatterns <= 0) {
            throw new IllegalArgumentException("maxPatterns must be positive, got: " + maxPatterns);
        }
        this.maxPatterns = maxPatterns;
        this.buffer = new ArrayDeque<>(Math.min(maxPatterns, 1024));
    }

    /**
     * Records a query pattern extracted from the optimized plan.
     *
     * @param pattern the observed query pattern
     */
    public void record(QueryPattern pattern) {
        if (pattern == null) {
            return;
        }
        lock.writeLock().lock();
        try {
            if (buffer.size() >= maxPatterns) {
                buffer.pollFirst();
            }
            buffer.addLast(pattern);
            LOG.debug("event=query_pattern_recorded table={} filters={} sorts={} size={}",
                    pattern.tableName(), pattern.filterColumns().size(),
                    pattern.sortColumns().size(), buffer.size());
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Returns all recorded patterns for the specified table.
     *
     * @param tableName fully qualified table name
     * @return patterns matching the table, may be empty
     */
    public List<QueryPattern> getPatternsForTable(String tableName) {
        if (tableName == null) {
            return List.of();
        }
        lock.readLock().lock();
        try {
            return buffer.stream()
                    .filter(p -> tableName.equals(p.tableName()))
                    .toList();
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * Returns the number of currently recorded patterns.
     *
     * @return recorded pattern count
     */
    public long size() {
        lock.readLock().lock();
        try {
            return buffer.size();
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * A structural query pattern extracted from an optimized plan.
     *
     * @param tableName     fully qualified table name
     * @param filterColumns columns used in WHERE predicates
     * @param joinColumns   columns used in JOIN conditions
     * @param sortColumns   columns used in ORDER BY
     * @param groupColumns  columns used in GROUP BY
     * @param isWrite       true if this was a DML (INSERT/UPDATE/DELETE) query
     * @param recordedAt    timestamp when the pattern was recorded
     */
    public record QueryPattern(
            String tableName,
            Set<String> filterColumns,
            Set<String> joinColumns,
            Set<String> sortColumns,
            Set<String> groupColumns,
            boolean isWrite,
            Instant recordedAt) {

        public QueryPattern {
            if (tableName == null || tableName.isBlank()) {
                throw new IllegalArgumentException("tableName required");
            }
            filterColumns = filterColumns != null ? Set.copyOf(filterColumns) : Set.of();
            joinColumns = joinColumns != null ? Set.copyOf(joinColumns) : Set.of();
            sortColumns = sortColumns != null ? Set.copyOf(sortColumns) : Set.of();
            groupColumns = groupColumns != null ? Set.copyOf(groupColumns) : Set.of();
            if (recordedAt == null) {
                throw new IllegalArgumentException("recordedAt required");
            }
        }
    }
}
