package org.pangrova.coordinator.ai.embedding;

import javax.annotation.concurrent.ThreadSafe;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

/**
 * In-memory implementation of {@link EmbeddingWorkStore} backed by a ConcurrentHashMap.
 *
 * <p>This implementation persists items in heap memory with full status tracking.
 * It is suitable for single-node deployments where "persistence" means surviving
 * a queue clear (e.g., for testing recovery logic) but not an actual JVM restart.</p>
 *
 * <p>For true crash-resilient persistence, use a JDBC-backed implementation.</p>
 */
@ThreadSafe
public final class InMemoryEmbeddingWorkStore implements EmbeddingWorkStore {

    private final ConcurrentHashMap<String, WorkEntry> entries = new ConcurrentHashMap<>();

    @Override
    public void save(PendingEmbeddingRow row) {
        Objects.requireNonNull(row, "row must not be null");
        entries.put(key(row.tableName(), row.rowId()), new WorkEntry(row, WorkItemStatus.PENDING));
    }

    @Override
    public void markInProgress(List<PendingEmbeddingRow> rows) {
        Objects.requireNonNull(rows, "rows must not be null");
        for (PendingEmbeddingRow row : rows) {
            String k = key(row.tableName(), row.rowId());
            entries.computeIfPresent(k, (key, existing) ->
                    new WorkEntry(existing.row, WorkItemStatus.IN_PROGRESS));
        }
    }

    @Override
    public void markCompleted(String tableName, String rowId) {
        Objects.requireNonNull(tableName, "tableName must not be null");
        Objects.requireNonNull(rowId, "rowId must not be null");
        entries.remove(key(tableName, rowId));
    }

    @Override
    public List<PendingEmbeddingRow> loadUnfinished() {
        return entries.values().stream()
                .filter(e -> e.status != WorkItemStatus.COMPLETED)
                .map(e -> e.row)
                .toList();
    }

    @Override
    public long countUnfinished(String tableName) {
        Objects.requireNonNull(tableName, "tableName must not be null");
        return entries.values().stream()
                .filter(e -> e.row.tableName().equals(tableName))
                .filter(e -> e.status != WorkItemStatus.COMPLETED)
                .count();
    }

    /**
     * Returns the current status of a work item (for test assertions).
     *
     * @param tableName the table name
     * @param rowId     the row identifier
     * @return the status, or null if no entry exists
     */
    public WorkItemStatus getStatus(String tableName, String rowId) {
        WorkEntry entry = entries.get(key(tableName, rowId));
        return entry != null ? entry.status : null;
    }

    /**
     * Returns the total number of entries (all statuses) in the store.
     */
    public int size() {
        return entries.size();
    }

    private static String key(String tableName, String rowId) {
        return tableName + "::" + rowId;
    }

    private record WorkEntry(PendingEmbeddingRow row, WorkItemStatus status) {
        WorkEntry {
            Objects.requireNonNull(row);
            Objects.requireNonNull(status);
        }
    }

    /**
     * Returns a snapshot of all entries for inspection (testing only).
     */
    Map<String, WorkItemStatus> snapshot() {
        var result = new ConcurrentHashMap<String, WorkItemStatus>();
        entries.forEach((k, v) -> result.put(k, v.status));
        return result;
    }
}
