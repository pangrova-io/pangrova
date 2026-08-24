package org.pangrova.spi.connector.metadata;

import org.pangrova.spi.connector.ConnectorSession;
import org.pangrova.spi.connector.ConnectorTransactionHandle;
import org.pangrova.spi.annotation.Stable;
import org.pangrova.spi.connector.metadata.table.ConnectorTableHandle;

/**
 * Manages the creation of splits for a connector table.
 *
 * <p>The engine calls {@link #getSplits} to obtain a {@link ConnectorSplitSource}
 * that enumerates the splits for a given table, allowing parallel data reading.</p>
 *
 * <p>Connectors that support incremental scanning (see
 * {@link IncrementalScanCapability}) may override
 * {@link #getIncrementalSplits} to return only the data files added
 * between two snapshots, enabling O(delta) refresh instead of O(total).</p>
 */
@Stable(since = "1.0")
public interface ConnectorSplitManager {

    /**
     * Returns a split source that enumerates splits for the specified table.
     *
     * @param transaction   the current transaction handle
     * @param session       the current session context
     * @param table         the table handle to generate splits for
     * @param dynamicFilter runtime dynamic filter from join operations
     * @param constraint    pushed-down predicate constraint
     * @return a split source for iterating over splits, never null
     */
    ConnectorSplitSource getSplits(
            ConnectorTransactionHandle transaction,
            ConnectorSession session,
            ConnectorTableHandle table,
            DynamicFilter dynamicFilter,
            Constraint constraint);

    /**
     * Returns a split source containing only data files added between two
     * snapshots (exclusive lower bound, inclusive upper bound).
     *
     * <p>The default implementation falls back to {@link #getSplits} for
     * backward compatibility with connectors that do not support
     * incremental scanning. Connectors that advertise
     * {@link IncrementalScanCapability#supportsIncrementalAppendScan()}
     * should override this method to return only the delta files.</p>
     *
     * @param session        the current session context
     * @param table          the table handle to generate splits for
     * @param fromSnapshotId exclusive lower-bound snapshot ID
     * @param toSnapshotId   inclusive upper-bound snapshot ID
     * @param constraint     pushed-down predicate constraint
     * @return a split source for iterating over incremental splits, never null
     */
    default ConnectorSplitSource getIncrementalSplits(
            ConnectorSession session,
            ConnectorTableHandle table,
            long fromSnapshotId,
            long toSnapshotId,
            Constraint constraint) {
        return getSplits(ConnectorTransactionHandle.EMPTY, session, table, DynamicFilter.ALL, constraint);
    }

    /**
     * Releases any resources held for the given query.
     * Called after query completion or cancellation to allow connectors to
     * free cached SDK objects (e.g., Paimon Split instances) that are
     * no longer needed.
     *
     * <p>Default implementation is a no-op for connectors that do not
     * retain per-query state.</p>
     *
     * @param queryId the query identifier whose resources should be released
     */
    default void cleanup(String queryId) {
        // no-op by default
    }
}
