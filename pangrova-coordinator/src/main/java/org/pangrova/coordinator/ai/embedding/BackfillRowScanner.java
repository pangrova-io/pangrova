package org.pangrova.coordinator.ai.embedding;

/**
 * Acquires a snapshot for backfill isolation.
 *
 * <p>The snapshot ensures only rows visible at backfill start are processed.
 * Rows inserted after the snapshot are handled by the next backfill invocation
 * or ASYNC drain.</p>
 */
public interface BackfillRowScanner {

    /**
     * Acquires the current snapshot identifier for the given table.
     *
     * <p>All subsequent row scanning for this backfill uses this snapshot,
     * providing snapshot isolation guarantees.</p>
     *
     * @param tableName the fully qualified table name
     * @return the snapshot identifier to use for row scanning
     */
    long acquireSnapshot(String tableName);
}
