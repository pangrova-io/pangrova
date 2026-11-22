package org.pangrova.connector.iceberg;

import org.pangrova.connector.api.enums.TableDeclarationEvidence;
import org.pangrova.connector.iceberg.handle.IcebergColumnHandle;
import org.pangrova.connector.iceberg.handle.IcebergTableHandle;
import org.pangrova.connector.iceberg.handle.IcebergTableHandleFactory;
import org.pangrova.connector.iceberg.type.IcebergTypeMapper;
import org.pangrova.connector.api.metadata.table.ColumnHandle;
import org.pangrova.connector.api.metadata.table.ConnectorTableHandle;
import org.pangrova.connector.api.metadata.table.ConnectorTableMetadata;
import org.pangrova.connector.api.metadata.table.ConnectorTableReadMetadata;
import org.pangrova.connector.api.metadata.table.SchemaTableName;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.SupportsNamespaces;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.jdbc.JdbcCatalog;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Catalog-side table resolution and listing for the Iceberg connector:
 * resolves tables from the catalog, resolves live (refreshed) tables from
 * handles, and lists schemas/tables via the Iceberg catalog API.
 */
final class IcebergCatalogTables {

    private static final Logger LOG = LoggerFactory.getLogger(IcebergCatalogTables.class);

    private final Catalog catalog;

    IcebergCatalogTables(Catalog catalog) {
        this.catalog = catalog;
    }

    /**
     * Resolves current catalog metadata. A direct JDBC catalog creates fresh
     * table operations on load, which already read the current metadata pointer.
     * Catalog wrappers and other implementations may return a cached table.
     */
    Table resolveTable(IcebergTableHandle handle) {
        return resolveTable(catalog, handle);
    }

    static Table resolveTable(Catalog catalog, IcebergTableHandle handle) {
        TableIdentifier id = TableIdentifier.of(handle.schemaName(), handle.tableName());
        Table table = catalog.loadTable(id);
        if (catalog.getClass() != JdbcCatalog.class) {
            table.refresh();
        }
        return table;
    }

    /** Loads a table handle, returning null when the table does not exist. */
    ConnectorTableHandle getTableHandle(SchemaTableName tableName) {
        TableIdentifier id = TableIdentifier.of(tableName.schema(), tableName.table());
        try {
            // A process-wide TTL cannot fence DROP/CREATE by another catalog owner.
            // Resolve existence and immutable handle metadata from the current catalog state.
            Table table = catalog.loadTable(id);
            return IcebergTableHandleFactory.fromTable(tableName.schema(), tableName.table(), table);
        } catch (org.apache.iceberg.exceptions.NoSuchTableException e) {
            LOG.debug("Table not found: {}", tableName);
            return null;
        }
    }

    /** Builds the connector table metadata for the given handle. */
    ConnectorTableMetadata getTableMetadata(IcebergTableHandle handle) {
        return getTableReadMetadata(handle).requireExactDeclaration();
    }

    ConnectorTableReadMetadata getTableReadMetadata(IcebergTableHandle handle) {
        return getTableReadMetadata(handle, resolveTable(handle));
    }

    ConnectorTableReadMetadata getTableReadMetadata(IcebergTableHandle handle, Table icebergTable) {
        var evidence = IcebergReadSchema.resolve(handle, icebergTable);
        return readDeclaration(handle, evidence);
    }

    static ConnectorTableReadMetadata readDeclaration(IcebergTableHandle handle, IcebergReadSchema evidence) {
        var properties = evidence.declarationProperties().orElse(Map.of());
        var typedColumns = IcebergTypeMapper.toPangrovaColumns(
                evidence.schema(), properties);
        List<String> columnNames = typedColumns.stream()
                .map(ConnectorTableMetadata.Column::name)
                .toList();
        SchemaTableName name = new SchemaTableName(handle.schemaName(), handle.tableName());
        return new ConnectorTableReadMetadata(new ConnectorTableMetadata(name, columnNames, typedColumns, properties),
                evidence.declarationProperties().isPresent() ? TableDeclarationEvidence.EXACT
                        : TableDeclarationEvidence.UNAVAILABLE);
    }

    /** Builds column handles from the table schema for the given handle. */
    Map<String, ColumnHandle> getColumnHandles(IcebergTableHandle handle) {
        var metadata = getTableMetadata(handle);
        Map<String, ColumnHandle> handles = new LinkedHashMap<>();
        for (var field : metadata.typedColumns()) {
            handles.put(field.name(), new IcebergColumnHandle(
                    field.fieldId().orElseThrow(), field.name(), field.type()));
        }
        return handles;
    }

    /** Lists first-level schema (namespace) names of the catalog. */
    List<String> listSchemaNames() {
        if (!(catalog instanceof SupportsNamespaces nsCatalog)) {
            return List.of();
        }
        return nsCatalog.listNamespaces().stream()
                .map(ns -> ns.level(0))
                .distinct()
                .toList();
    }

    /** Lists tables in one schema, or across all schemas when absent. */
    List<SchemaTableName> listTables(Optional<String> schemaName) {
        return schemaName
                .map(this::listTablesInSchema)
                .orElseGet(this::listAllTables);
    }

    private List<SchemaTableName> listTablesInSchema(String schema) {
        Namespace ns = Namespace.of(schema);
        try {
            return catalog.listTables(ns).stream()
                    .map(id -> new SchemaTableName(schema, id.name()))
                    .toList();
        } catch (org.apache.iceberg.exceptions.NoSuchNamespaceException e) {
            LOG.debug("Namespace not found: {}", schema);
            return List.of();
        }
    }

    private List<SchemaTableName> listAllTables() {
        return listSchemaNames().stream()
                .flatMap(schema -> listTablesInSchema(schema).stream())
                .toList();
    }
}
