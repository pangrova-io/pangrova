package org.pangrova.connector.paimon.write;

import org.pangrova.common.exception.PangrovaErrorCode;
import org.pangrova.connector.spi.base.ConnectorException;
import org.pangrova.connector.paimon.handle.PaimonOutputTableHandle;
import org.pangrova.connector.paimon.handle.PaimonTableIdentity;
import org.pangrova.connector.spi.ConnectorSession;
import org.pangrova.connector.spi.ConnectorTransactionHandle;
import org.pangrova.connector.spi.data.ConnectorPageSink;
import org.pangrova.connector.spi.data.ConnectorPageSinkProvider;
import org.pangrova.connector.spi.metadata.ConnectorOutputTableHandle;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.table.Table;

/**
 * Creates PaimonPageSink instances for writing data to Paimon tables.
 * Uses Table.newBatchWriteBuilder() for batch write operations.
 *
 * <p>Always loads a fresh Table from catalog to ensure write operations
 * use the latest table metadata and schema.
 */
public class PaimonPageSinkProvider implements ConnectorPageSinkProvider {

    private final Catalog catalog;
    private final String tempDir;

    /** Test-only constructor with default temp directory. */
    public PaimonPageSinkProvider(Catalog catalog) {
        this(catalog, System.getProperty("java.io.tmpdir") + "/paimon-io");
    }

    public PaimonPageSinkProvider(Catalog catalog, String tempDir) {
        this.catalog = catalog;
        this.tempDir = tempDir;
    }

    @Override
    public ConnectorPageSink createPageSink(
            ConnectorTransactionHandle transaction,
            ConnectorSession session,
            ConnectorOutputTableHandle outputTableHandle) {
        PaimonOutputTableHandle handle = (PaimonOutputTableHandle) outputTableHandle;
        Table freshTable = loadFreshTable(handle);
        return new PaimonPageSink(freshTable, handle.schemaName(),
                handle.tableName(), tempDir);
    }

    @Override
    public ConnectorPageSink createOverwritePageSink(
            ConnectorTransactionHandle transaction,
            ConnectorSession session,
            ConnectorOutputTableHandle outputTableHandle) {
        PaimonOutputTableHandle handle = (PaimonOutputTableHandle) outputTableHandle;
        Table freshTable = loadFreshTable(handle);
        return new PaimonOverwritePageSink(freshTable, handle.schemaName(),
                handle.tableName(), tempDir, handle.overwritePartition());
    }

    @Override
    public ConnectorPageSink createDeletePageSink(
            ConnectorTransactionHandle transaction,
            ConnectorSession session,
            ConnectorOutputTableHandle outputTableHandle) {
        PaimonOutputTableHandle handle = (PaimonOutputTableHandle) outputTableHandle;
        Table freshTable = loadFreshTable(handle);
        return new PaimonDeletePageSink(freshTable, handle.schemaName(),
                handle.tableName(), tempDir);
    }

    /**
     * Loads a fresh table from the catalog and fences a drop/recreate or schema change that
     * happened after the write was planned.
     */
    private Table loadFreshTable(PaimonOutputTableHandle handle) {
        Identifier id = Identifier.create(handle.schemaName(), handle.tableName());
        Table fresh;
        try {
            fresh = PaimonTableIdentity.loadFresh(catalog, id);
        } catch (Catalog.TableNotExistException e) {
            throw new ConnectorException(PangrovaErrorCode.CONNECTOR_TABLE_NOT_FOUND,
                    "Table not found for write: " + id, e);
        }
        return PaimonTableIdentity.requireSame(fresh, handle.tableUuid(), handle.tableLocation(),
                handle.schemaFingerprint(), "Paimon write");
    }
}
