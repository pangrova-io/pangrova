package org.pangrova.metadata.adapter;

import org.pangrova.metadata.model.ColumnMetadata;
import org.pangrova.metadata.model.TableMetadata;
import org.pangrova.spi.connector.metadata.ConnectorMetadata;
import org.pangrova.spi.connector.ConnectorSession;
import org.pangrova.spi.connector.metadata.table.ColumnHandle;
import org.pangrova.spi.connector.metadata.table.ConnectorTableHandle;
import org.pangrova.spi.connector.metadata.table.ConnectorTableMetadata;
import org.pangrova.spi.connector.metadata.table.SchemaTableName;
import org.pangrova.spi.connector.metadata.table.TableStatistics;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Read-only ConnectorMetadata that delegates to a CatalogAdapter.
 * All DDL operations throw UnsupportedOperationException.
 */
public class FederationConnectorMetadata implements ConnectorMetadata {

    private final FederatedCatalog adapter;

    public FederationConnectorMetadata(FederatedCatalog adapter) {
        this.adapter = adapter;
    }

    @Override
    public List<String> listSchemaNames(ConnectorSession session) {
        return adapter.listSchemas();
    }

    @Override
    public List<SchemaTableName> listTables(ConnectorSession session, Optional<String> schemaName) {
        return schemaName
                .map(this::listTablesInSchema)
                .orElseGet(this::listAllTables);
    }

    @Override
    public ConnectorTableHandle getTableHandle(ConnectorSession session, SchemaTableName tableName) {
        TableMetadata metadata = adapter.getTable(tableName.schema(), tableName.table());
        return metadata == null ? null : new FederationTableHandle(tableName.schema(), tableName.table());
    }

    @Override
    public ConnectorTableMetadata getTableMetadata(ConnectorSession session, ConnectorTableHandle table) {
        FederationTableHandle handle = (FederationTableHandle) table;
        TableMetadata metadata = adapter.getTable(handle.schema(), handle.table());
        return toConnectorTableMetadata(handle, metadata);
    }

    @Override
    public Map<String, ColumnHandle> getColumnHandles(ConnectorSession session, ConnectorTableHandle table) {
        FederationTableHandle handle = (FederationTableHandle) table;
        TableMetadata metadata = adapter.getTable(handle.schema(), handle.table());
        if (metadata == null) {
            return Map.of();
        }
        Map<String, ColumnHandle> result = new LinkedHashMap<>();
        for (ColumnMetadata column : metadata.columns()) {
            result.put(column.name(), new FederationColumnHandle(column.name(), column.type().baseType()));
        }
        return Map.copyOf(result);
    }

    @Override
    public TableStatistics getTableStatistics(ConnectorSession session, ConnectorTableHandle table) {
        FederationTableHandle handle = (FederationTableHandle) table;
        return adapter.getTableStatistics(handle.schema(), handle.table());
    }

    private List<SchemaTableName> listTablesInSchema(String schema) {
        return adapter.listTables(schema).stream()
                .map(t -> new SchemaTableName(schema, t))
                .toList();
    }

    private List<SchemaTableName> listAllTables() {
        return adapter.listSchemas().stream()
                .flatMap(s -> listTablesInSchema(s).stream())
                .toList();
    }

    private ConnectorTableMetadata toConnectorTableMetadata(FederationTableHandle handle, TableMetadata metadata) {
        List<String> columnNames = metadata.columns().stream()
                .map(ColumnMetadata::name)
                .toList();
        SchemaTableName tableName = new SchemaTableName(handle.schema(), handle.table());
        return new ConnectorTableMetadata(tableName, columnNames, metadata.properties());
    }
}
