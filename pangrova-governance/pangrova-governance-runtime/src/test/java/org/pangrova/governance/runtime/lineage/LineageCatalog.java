package org.pangrova.governance.runtime.lineage;

import org.pangrova.common.concurrent.ThreadSafe;
import org.pangrova.governance.api.lineage.LineageRecord;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * In-memory lineage catalog that stores column-level lineage graphs per query.
 * Provides query APIs for retrieving lineage by query ID or by table/column.
 *
 * <p>Concurrency strategy: ConcurrentHashMap for thread-safe read/write without locking.</p>
 */
@ThreadSafe
public final class LineageCatalog {

    /** Default capacity for catalogs created without an explicit limit. */
    private static final int DEFAULT_MAX_ENTRIES = 10_000;

    private final ConcurrentHashMap<String, LineageRecord> recordsByQueryId = new ConcurrentHashMap<>();
    private final int maxEntries;

    /**
     * Creates a catalog with a maximum entry limit to bound memory usage.
     *
     * @param maxEntries maximum number of lineage records to retain (oldest evicted on overflow)
     */
    public LineageCatalog(int maxEntries) {
        if (maxEntries <= 0) {
            throw new IllegalArgumentException("maxEntries must be positive");
        }
        this.maxEntries = maxEntries;
    }

    /**
     * Creates a catalog with default capacity (10,000 entries).
     */
    public LineageCatalog() {
        this(DEFAULT_MAX_ENTRIES);
    }

    /**
     * Stores a lineage record for a completed query.
     *
     * @param record the lineage record to store
     */
    public void store(LineageRecord record) {
        if (recordsByQueryId.size() >= maxEntries) {
            evictOldest();
        }
        recordsByQueryId.put(record.queryId(), record);
    }

    /**
     * Retrieves lineage for a specific query.
     *
     * @param queryId the query identifier
     * @return the lineage record, or empty if not found
     */
    public Optional<LineageRecord> findByQueryId(String queryId) {
        return Optional.ofNullable(recordsByQueryId.get(queryId));
    }

    /**
     * Finds all lineage records that reference a given table as a source.
     *
     * @param tableName fully qualified table name
     * @return list of matching lineage records
     */
    public List<LineageRecord> findBySourceTable(String tableName) {
        return recordsByQueryId.values().stream()
                .filter(r -> r.sourceTables().contains(tableName))
                .toList();
    }

    /**
     * Finds all lineage records where a specific column is referenced as a source.
     *
     * @param tableName  fully qualified table name
     * @param columnName column name within the table
     * @return list of matching lineage records
     */
    public List<LineageRecord> findBySourceColumn(String tableName, String columnName) {
        return recordsByQueryId.values().stream()
                .filter(r -> r.hasSourceColumn(tableName, columnName))
                .toList();
    }

    /**
     * Returns the current number of stored lineage records.
     */
    public int size() {
        return recordsByQueryId.size();
    }

    /**
     * Returns an unmodifiable view of all stored records.
     */
    public Map<String, LineageRecord> allRecords() {
        return Collections.unmodifiableMap(recordsByQueryId);
    }

    private void evictOldest() {
        recordsByQueryId.entrySet().stream()
                .min(Map.Entry.comparingByValue(
                        (a, b) -> Long.compare(a.timestampEpochMs(), b.timestampEpochMs())))
                .ifPresent(entry -> recordsByQueryId.remove(entry.getKey()));
    }
}
