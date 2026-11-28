package org.pangrova.connector.paimon.write;

import org.pangrova.common.enums.PangrovaErrorCode;
import org.pangrova.connector.api.exception.ConnectorException;
import org.pangrova.connector.paimon.handle.PaimonOutputTableHandle;
import org.pangrova.connector.paimon.handle.PaimonTableIdentity;
import org.pangrova.connector.api.session.ConnectorSession;
import org.pangrova.connector.api.transaction.ConnectorTransactionHandle;
import org.pangrova.connector.api.data.ConnectorPageSink;
import org.pangrova.connector.api.data.ConnectorPageSinkProvider;
import org.pangrova.connector.api.metadata.ConnectorOutputTableHandle;
import org.apache.paimon.CoreOptions;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.Table;

import java.util.Map;

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
        FileStoreTable freshTable = loadFreshTable(handle);
        return new PaimonPageSink(freshTable, handle, tempDir);
    }

    @Override
    public org.pangrova.connector.api.data.ConnectorCdcPageSink createCdcPageSink(
            ConnectorTransactionHandle transaction,
            ConnectorSession session,
            ConnectorOutputTableHandle outputTableHandle) {
        PaimonOutputTableHandle handle = (PaimonOutputTableHandle) outputTableHandle;
        FileStoreTable freshTable = loadFreshTable(handle);
        return new PaimonCdcPageSink(freshTable, handle, tempDir);
    }

    @Override
    public ConnectorPageSink createOverwritePageSink(
            ConnectorTransactionHandle transaction,
            ConnectorSession session,
            ConnectorOutputTableHandle outputTableHandle) {
        PaimonOutputTableHandle handle = (PaimonOutputTableHandle) outputTableHandle;
        FileStoreTable freshTable = loadFreshTable(handle);
        return new PaimonOverwritePageSink(freshTable, handle, tempDir);
    }

    @Override
    public ConnectorPageSink createDeletePageSink(
            ConnectorTransactionHandle transaction,
            ConnectorSession session,
            ConnectorOutputTableHandle outputTableHandle) {
        PaimonOutputTableHandle handle = (PaimonOutputTableHandle) outputTableHandle;
        FileStoreTable freshTable = loadFreshTable(handle);
        return new PaimonDeletePageSink(freshTable, handle, tempDir);
    }

    /**
     * Loads a fresh table from the catalog and fences a drop/recreate or schema change that
     * happened after the write was planned.
     */
    private FileStoreTable loadFreshTable(PaimonOutputTableHandle handle) {
        Identifier id = Identifier.create(handle.schemaName(), handle.tableName());
        Table fresh;
        try {
            fresh = PaimonTableIdentity.loadFresh(catalog, id);
        } catch (Catalog.TableNotExistException e) {
            throw new ConnectorException(PangrovaErrorCode.CONNECTOR_TABLE_NOT_FOUND,
                    "Table not found for write: " + id, e);
        }
        var table = PaimonTableIdentity.requireSame(fresh, handle.tableUuid(), handle.tableLocation(),
                handle.schemaFingerprint(), "Paimon write");
        var staged = handle.stagedFileNames();
        return table.copy(Map.of(CoreOptions.DATA_FILE_PREFIX.key(), staged.dataPrefix(),
                CoreOptions.CHANGELOG_FILE_PREFIX.key(), staged.changelogPrefix()));
    }
}
