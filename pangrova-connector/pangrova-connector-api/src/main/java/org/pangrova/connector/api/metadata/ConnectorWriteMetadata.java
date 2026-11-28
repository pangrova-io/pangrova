package org.pangrova.connector.api.metadata;

import org.pangrova.connector.api.enums.DeletionMode;

import org.pangrova.connector.api.commit.DistributedWriteCommitContext;

import org.pangrova.common.api.annotation.Stable;
import org.pangrova.common.enums.PangrovaErrorCode;
import org.pangrova.connector.api.exception.ConnectorException;
import org.pangrova.connector.api.session.ConnectorSession;
import org.pangrova.connector.api.metadata.table.ConnectorTableHandle;
import org.pangrova.connector.api.metadata.table.ConnectorTableMetadata;

/**
 * Write-specific metadata operations for a connector.
 *
 * <p>Extracted from {@link ConnectorMetadata} per Interface Segregation Principle.
 * Connectors that are read-only need not implement these methods beyond the
 * default no-op/unsupported stubs.</p>
 */
@Stable(since = "1.0")
public interface ConnectorWriteMetadata {

    /** Creates a new table with the given metadata. */
    default ConnectorOutputTableHandle createTable(ConnectorSession session, ConnectorTableMetadata metadata) {
        throw new UnsupportedOperationException("createTable not supported");
    }

    /** Begins an insert operation on an existing table. */
    default ConnectorOutputTableHandle beginInsert(ConnectorSession session, ConnectorTableHandle table) {
        throw new UnsupportedOperationException("beginInsert not supported");
    }

    /** Begins a delete operation on an existing table. */
    default ConnectorOutputTableHandle beginDelete(ConnectorSession session, ConnectorTableHandle table) {
        throw new UnsupportedOperationException("beginDelete not supported");
    }

    /** Whether this connector supports targeted (row-level) delete. */
    default boolean supportsTargetedDelete() {
        return false;
    }

    /**
     * Returns the deletion mode used by this connector for the given table.
     *
     * <p>The coordinator uses this to determine the correct write path
     * and commit protocol for DELETE operations.</p>
     *
     * @param session the current session context
     * @param table   the target table handle
     * @return the deletion mode, defaulting to {@link DeletionMode#POSITION_DELETE}
     */
    default DeletionMode deletionMode(ConnectorSession session, ConnectorTableHandle table) {
        return DeletionMode.POSITION_DELETE;
    }

    /**
     * Deletes rows matching the given filter expression.
     *
     * <p>Executes entirely in Java via the connector's native delete API
     * (e.g., Iceberg {@code deleteFromRowFilter}, Paimon predicate delete).
     * No native worker involvement — the connector handles the delete
     * at the metadata/storage layer.</p>
     *
     * @param session the current session context
     * @param table   the target table handle
     * @param filter  the delete filter expression (WHERE clause)
     */
    default void deleteByFilter(ConnectorSession session,
                                ConnectorTableHandle table,
                                DeleteFilter filter) {
        throw new UnsupportedOperationException("deleteByFilter not supported");
    }

    /** Begins a CDC merge operation that stages deletes and inserts for one atomic commit. */
    default ConnectorOutputTableHandle beginMerge(ConnectorSession session, ConnectorTableHandle table) {
        throw new UnsupportedOperationException("beginMerge not supported");
    }

    /** Allocates a row-kind-aware CDC sink against the exact target table. */
    default ConnectorOutputTableHandle beginCdcMerge(ConnectorSession session, ConnectorTableHandle table) {
        return beginMerge(session, table);
    }

    /** Whether merge handles can stage and replay before and after images through the CDC sink. */
    default boolean supportsCdcMutations() {
        return false;
    }

    /** Begins an overwrite operation that atomically replaces all data in a table. */
    default ConnectorOutputTableHandle beginOverwrite(ConnectorSession session, ConnectorTableHandle table) {
        throw new UnsupportedOperationException("beginOverwrite not supported");
    }

    /**
     * Begins a static partition overwrite that replaces data only in the specified partitions.
     *
     * <p>Equivalent to {@code INSERT OVERWRITE T PARTITION (key=val, ...)} semantics.
     * Only the partitions identified by {@code staticPartition} are replaced; all other
     * partitions remain untouched.</p>
     *
     * <p>Default delegates to the unfiltered {@link #beginOverwrite} for connectors
     * that handle partition scoping via the written data (dynamic partition overwrite).</p>
     *
     * @param session         the current session context
     * @param table           the target table handle
     * @param staticPartition partition column name-value pairs identifying the target partitions
     * @return an output table handle for the overwrite operation
     */
    default ConnectorOutputTableHandle beginOverwrite(ConnectorSession session,
                                                      ConnectorTableHandle table,
                                                      java.util.Map<String, String> staticPartition) {
        return beginOverwrite(session, table);
    }

    /** Drops an existing table. */
    default void dropTable(ConnectorSession session, ConnectorTableHandle table) {
        throw new UnsupportedOperationException("dropTable not supported");
    }

    /**
     * Commits a distributed write using commit metadata fragments from workers.
     *
     * <p>Each worker produces commit metadata via
     * {@link org.pangrova.connector.api.data.ConnectorPageSink#finishWithMetadata()}
     * without performing a catalog commit. The coordinator collects all fragments
     * and calls this method to perform a single atomic catalog commit.</p>
     *
     * <p>For Iceberg: deserializes DataFile descriptors from fragments and
     * performs a single {@code table.newAppend().appendFile(...).commit()}.</p>
     *
     * <p>For Paimon: deserializes CommitMessage list from fragments and
     * performs a single {@code tableCommit.commit(messages)}.</p>
     *
     * <p>This method must be idempotent: if the commit already happened
     * (e.g., coordinator crash recovery), calling again with the same
     * fragments must succeed without creating duplicate data.</p>
     *
     * @param session       the current session context
     * @param outputHandle  handle from beginInsert/beginOverwrite/beginDelete
     * @param fragments     commit metadata from each worker's finishWithMetadata()
     */
    default void finishDistributedWrite(ConnectorSession session,
                                        ConnectorOutputTableHandle outputHandle,
                                        java.util.List<byte[]> fragments) {
        throw new UnsupportedOperationException("finishDistributedWrite not supported");
    }

    /**
     * Commits a journaled distributed write with a stable identity shared by the first attempt and
     * crash recovery. A connector must explicitly implement the replay-safe commit contract;
     * falling back to the context-free overload could publish the same write twice after a crash.
     */
    default void finishDistributedWrite(ConnectorSession session,
                                        ConnectorOutputTableHandle outputHandle,
                                        java.util.List<byte[]> fragments,
                                        DistributedWriteCommitContext context) {
        throw new ConnectorException(PangrovaErrorCode.CONNECTOR_WRITE_ERROR,
                "Replay-safe distributed write commit is not implemented by this connector");
    }

    /**
     * Installs a durable abort barrier for the exact journaled intent. Returning proves
     * that this intent can never become visible; it does not authorize object deletion.
     */
    default DistributedWriteAbortReceipt abortDistributedWrite(ConnectorSession session,
            ConnectorOutputTableHandle outputHandle, java.util.List<byte[]> fragments,
            DistributedWriteCommitContext context) {
        throw new UnsupportedOperationException("Durable distributed write abort is not implemented");
    }
}
