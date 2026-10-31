package org.pangrova.connector.spi.data;

import org.pangrova.connector.spi.ConnectorSession;
import org.pangrova.connector.spi.ConnectorTransactionHandle;
import org.pangrova.common.api.annotation.Stable;
import org.pangrova.connector.spi.metadata.ConnectorOutputTableHandle;

/**
 * Creates {@link ConnectorPageSink} instances for writing data to output tables.
 *
 * <p>The engine calls {@link #createPageSink} to obtain a sink for writing
 * data to the underlying data store.</p>
 */
@Stable(since = "1.0")
public interface ConnectorPageSinkProvider {

    /**
     * Creates a page sink for writing data to the specified output table.
     *
     * @param transaction       the current transaction handle
     * @param session           the current session context
     * @param outputTableHandle the output table handle
     * @return a page sink for writing data, never null
     */
    ConnectorPageSink createPageSink(
            ConnectorTransactionHandle transaction,
            ConnectorSession session,
            ConnectorOutputTableHandle outputTableHandle);

    /**
     * Creates a page sink that atomically overwrites all data in the target table.
     * Used by copy-on-write UPDATE/DELETE/MERGE implementations.
     *
     * <p>Compatibility: additive default method — no migration required for existing providers.
     *
     * @param transaction       the current transaction handle
     * @param session           the current session context
     * @param outputTableHandle the output table handle
     * @return a page sink for overwriting data, never null
     */
    default ConnectorPageSink createOverwritePageSink(
            ConnectorTransactionHandle transaction,
            ConnectorSession session,
            ConnectorOutputTableHandle outputTableHandle) {
        throw new UnsupportedOperationException("createOverwritePageSink not supported");
    }

    /**
     * Creates a page sink for writing delete markers to the target table.
     * Used by merge-on-read DELETE implementations on primary key tables.
     *
     * <p>For Paimon: writes rows with {@code RowKind.DELETE} so the LSM engine
     * marks matching primary key rows as deleted during compaction.</p>
     *
     * <p>For Iceberg: writes position/equality delete files via the v2 format.</p>
     *
     * <p>Default delegates to {@link #createPageSink} for connectors that handle
     * delete semantics transparently (e.g., via Substrait plan execution).</p>
     *
     * @param transaction       the current transaction handle
     * @param session           the current session context
     * @param outputTableHandle the output table handle
     * @return a page sink for writing delete markers, never null
     */
    default ConnectorPageSink createDeletePageSink(
            ConnectorTransactionHandle transaction,
            ConnectorSession session,
            ConnectorOutputTableHandle outputTableHandle) {
        return createPageSink(transaction, session, outputTableHandle);
    }

}
