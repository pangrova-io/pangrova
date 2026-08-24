package org.pangrova.metadata.dynamictable;

import java.time.Instant;
import java.util.List;

/**
 * Persistent store for Dynamic Table refresh history entries.
 *
 * <p>Implementations must be thread-safe. Entries are keyed by
 * {@link RefreshHistoryEntry#dtName()} and ordered by start time.</p>
 *
 * <p>Supports the {@code information_schema.dynamic_table_refresh_history}
 * virtual table (R23 AC2) and automatic purge of stale entries (R23 AC3).</p>
 */
public interface RefreshHistoryStore {

    /**
     * Persist a refresh history entry.
     *
     * @param entry the completed refresh record to store
     */
    void record(RefreshHistoryEntry entry);

    /**
     * Query refresh history for a specific Dynamic Table within a time range.
     *
     * <p>Results are ordered by {@link RefreshHistoryEntry#startTime()} descending
     * (most recent first) and capped at {@code limit}.</p>
     *
     * @param dtName Dynamic Table name to filter by
     * @param from   inclusive lower bound on start time
     * @param to     inclusive upper bound on start time
     * @param limit  maximum number of entries to return
     * @return unmodifiable list of matching entries, most recent first
     */
    List<RefreshHistoryEntry> query(String dtName, Instant from, Instant to, int limit);

    /**
     * Paginated query for refresh history.
     *
     * @param dtName Dynamic Table name to filter by
     * @param from   inclusive lower bound on start time
     * @param to     inclusive upper bound on start time
     * @param limit  maximum number of entries to return
     * @param offset number of entries to skip (for pagination)
     * @return unmodifiable list of matching entries, most recent first
     */
    default List<RefreshHistoryEntry> query(String dtName, Instant from, Instant to,
                                            int limit, int offset) {
        // Default: ignore offset, delegate to non-paginated query.
        // Implementations should override for efficient offset support.
        List<RefreshHistoryEntry> all = query(dtName, from, to, limit + offset);
        if (offset >= all.size()) {
            return List.of();
        }
        return List.copyOf(all.subList(offset, Math.min(offset + limit, all.size())));
    }

    /**
     * Purge stale history entries.
     *
     * <p>For each Dynamic Table, removes entries older than {@code olderThan}
     * and retains at most {@code maxEntries} per Dynamic Table (keeping the
     * most recent ones).</p>
     *
     * @param olderThan entries with start time before this instant are eligible for removal
     * @param maxEntries maximum entries to retain per Dynamic Table
     */
    void purge(Instant olderThan, int maxEntries);
}
