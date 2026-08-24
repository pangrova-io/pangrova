package org.pangrova.governance.lineage;

import org.pangrova.common.concurrent.ThreadSafe;

import java.util.List;
import java.util.Optional;

/**
 * Public API for querying column-level lineage stored in the governance catalog.
 * Delegates to {@link LineageCatalog} for storage and retrieval.
 *
 * <p>Concurrency strategy: delegates to thread-safe LineageCatalog.</p>
 */
@ThreadSafe
public final class LineageQueryService {

    private final LineageCatalog catalog;

    /**
     * Creates a query service delegating to the given lineage catalog.
     */
    public LineageQueryService(LineageCatalog catalog) {
        if (catalog == null) {
            throw new IllegalArgumentException("catalog must not be null");
        }
        this.catalog = catalog;
    }

    /**
     * Retrieves the full lineage record for a specific query execution.
     *
     * @param queryId the query identifier
     * @return lineage record if found
     */
    public Optional<LineageRecord> getLineageByQueryId(String queryId) {
        return catalog.findByQueryId(queryId);
    }

    /**
     * Finds all queries that read from a specific table.
     *
     * @param tableName fully qualified table name
     * @return list of lineage records referencing this table as source
     */
    public List<LineageRecord> getLineageBySourceTable(String tableName) {
        return catalog.findBySourceTable(tableName);
    }

    /**
     * Finds all queries that read a specific column from a table.
     * Useful for impact analysis: "who uses this column?"
     *
     * @param tableName  fully qualified table name
     * @param columnName column name
     * @return list of lineage records referencing this column
     */
    public List<LineageRecord> getLineageBySourceColumn(String tableName, String columnName) {
        return catalog.findBySourceColumn(tableName, columnName);
    }

    /**
     * Returns the total number of lineage records stored.
     */
    public int totalRecords() {
        return catalog.size();
    }
}
