package org.pangrova.connector.iceberg.write;

import org.pangrova.connector.iceberg.handle.IcebergOutputTableHandle;
import org.pangrova.connector.iceberg.handle.IcebergTableHandleFactory;
import org.pangrova.connector.api.session.ConnectorSession;
import org.pangrova.connector.api.transaction.ConnectorTransactionHandle;
import org.pangrova.connector.spi.data.ConnectorPageSink;
import org.pangrova.connector.spi.data.ConnectorPageSinkProvider;
import org.pangrova.connector.api.metadata.ConnectorOutputTableHandle;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;

/**
 * Creates IcebergPageSink instances for writing data to Iceberg tables.
 *
 * <p>Requires a Catalog reference to resolve live Table objects from immutable handles.
 * Optionally accepts a commit callback for notifying the indexing pipeline.
 */
public class IcebergPageSinkProvider implements ConnectorPageSinkProvider {

    private final Catalog catalog;

    public IcebergPageSinkProvider(Catalog catalog) {
        this.catalog = catalog;
    }

    @Override
    public ConnectorPageSink createPageSink(
            ConnectorTransactionHandle transaction,
            ConnectorSession session,
            ConnectorOutputTableHandle outputTableHandle) {
        IcebergOutputTableHandle handle = (IcebergOutputTableHandle) outputTableHandle;
        Table table = resolveTable(handle);
        Schema writeSchema = IcebergTableHandleFactory.parseSchema(handle);
        return new IcebergPageSink(
                new IcebergWriteContext(table, writeSchema, handle.schemaName(), handle.tableName()));
    }

    @Override
    public ConnectorPageSink createOverwritePageSink(
            ConnectorTransactionHandle transaction,
            ConnectorSession session,
            ConnectorOutputTableHandle outputTableHandle) {
        IcebergOutputTableHandle handle = (IcebergOutputTableHandle) outputTableHandle;
        Table table = resolveTable(handle);
        Schema writeSchema = IcebergTableHandleFactory.parseSchema(handle);
        return new IcebergOverwritePageSink(
                new IcebergWriteContext(table, writeSchema, handle.schemaName(), handle.tableName()),
                handle.baseSnapshotId());
    }

    @Override
    public ConnectorPageSink createDeletePageSink(
            ConnectorTransactionHandle transaction,
            ConnectorSession session,
            ConnectorOutputTableHandle outputTableHandle) {
        // Copy-on-write DELETE: the native worker reads all rows, applies the
        // WHERE filter, and writes surviving rows through an overwrite sink.
        // For DELETE without WHERE, zero rows survive and the overwrite sink
        // commits with empty data files, which deletes all rows.
        return createOverwritePageSink(transaction, session, outputTableHandle);
    }

    /**
     * Resolves a live Iceberg Table from the catalog using handle metadata.
     */
    private Table resolveTable(IcebergOutputTableHandle handle) {
        TableIdentifier id = TableIdentifier.of(handle.schemaName(), handle.tableName());
        Table table = catalog.loadTable(id);
        table.refresh();
        java.util.UUID uuid = table.uuid();
        if (uuid == null || !handle.tableUuid().equals(uuid.toString())) {
            throw new IllegalStateException(
                    "Iceberg table UUID changed since the distributed write was planned");
        }
        return table;
    }
}
