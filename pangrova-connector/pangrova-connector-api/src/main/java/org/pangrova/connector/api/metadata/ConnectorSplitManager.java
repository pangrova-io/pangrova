package org.pangrova.connector.api.metadata;

import org.pangrova.connector.api.session.ConnectorSession;
import org.pangrova.connector.api.enums.ReadSnapshotState;
import org.pangrova.connector.api.transaction.ConnectorTransactionHandle;
import org.pangrova.common.api.annotation.Stable;
import org.pangrova.connector.api.metadata.table.ConnectorReadBinding;
import org.pangrova.connector.api.metadata.table.ConnectorTableHandle;

/**
 * Manages the creation of splits for a connector table.
 *
 * <p>The engine calls {@link #getSplits} to obtain a {@link ConnectorSplitSource}
 * that enumerates the splits for a given table, allowing parallel data reading.</p>
 *
 * <p>Incremental scans use a selected read whose handle carries a snapshot range.</p>
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
     * Opens a scan for a nonempty, selected read. A backend that can validate
     * the binding against the same table observation used for enumeration may
     * override this operation and perform the two steps together.
     */
    default ConnectorSplitSource getBoundSplits(ConnectorTransactionHandle transaction,
            ConnectorSession session, ConnectorMetadata metadata, ConnectorReadBinding binding,
            DynamicFilter dynamicFilter, Constraint constraint, SplitPlanningBudget budget) {
        budget.check(0);
        if (binding.state() == ReadSnapshotState.EMPTY) {
            throw new IllegalArgumentException("An empty binding has no splits to enumerate");
        }
        metadata.validateReadBinding(session, binding);
        return getSplits(transaction, session, binding.handle(), dynamicFilter, constraint);
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
