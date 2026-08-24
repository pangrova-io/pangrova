package org.pangrova.connector.paimon.write;

import org.pangrova.common.exception.PangrovaErrorCode;
import org.pangrova.spi.connector.base.ConnectorException;
import org.pangrova.connector.paimon.handle.PaimonOutputTableHandle;
import org.pangrova.spi.connector.ConnectorSession;
import org.pangrova.spi.connector.ConnectorTransactionHandle;
import org.pangrova.spi.connector.data.ConnectorPageSink;
import org.pangrova.spi.connector.data.ConnectorPageSinkProvider;
import org.pangrova.spi.connector.metadata.ConnectorOutputTableHandle;
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
        Table freshTable = loadFreshTable(handle.schemaName(), handle.tableName());
        return new PaimonPageSink(freshTable, handle.schemaName(),
                handle.tableName(), tempDir);
    }

    @Override
    public ConnectorPageSink createOverwritePageSink(
            ConnectorTransactionHandle transaction,
            ConnectorSession session,
            ConnectorOutputTableHandle outputTableHandle) {
        PaimonOutputTableHandle handle = (PaimonOutputTableHandle) outputTableHandle;
        Table freshTable = loadFreshTable(handle.schemaName(), handle.tableName());
        return new PaimonOverwritePageSink(freshTable, handle.schemaName(),
                handle.tableName(), tempDir, handle.overwritePartition());
    }

    @Override
    public ConnectorPageSink createDeletePageSink(
            ConnectorTransactionHandle transaction,
            ConnectorSession session,
            ConnectorOutputTableHandle outputTableHandle) {
        PaimonOutputTableHandle handle = (PaimonOutputTableHandle) outputTableHandle;
        Table freshTable = loadFreshTable(handle.schemaName(), handle.tableName());
        return new PaimonDeletePageSink(freshTable, handle.schemaName(),
                handle.tableName(), tempDir);
    }

    /**
     * Loads a fresh table from the catalog.
     * Ensures write operations use the latest table metadata.
     */
    private Table loadFreshTable(String schemaName, String tableName) {
        Identifier id = Identifier.create(schemaName, tableName);
        try {
            return catalog.getTable(id);
        } catch (Catalog.TableNotExistException e) {
            throw new ConnectorException(PangrovaErrorCode.CONNECTOR_TABLE_NOT_FOUND,
                    "Table not found for write: " + id, e);
        }
    }
}
