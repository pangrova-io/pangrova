package org.pangrova.connector.iceberg;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import org.pangrova.connector.iceberg.handle.IcebergColumnHandle;
import org.pangrova.connector.iceberg.handle.IcebergTableHandle;
import org.pangrova.connector.iceberg.handle.IcebergTableHandleFactory;
import org.pangrova.connector.iceberg.type.IcebergTypeMapper;
import org.pangrova.spi.connector.data.type.PangrovaType;
import org.pangrova.spi.connector.metadata.table.ColumnHandle;
import org.pangrova.spi.connector.metadata.table.ConnectorTableHandle;
import org.pangrova.spi.connector.metadata.table.ConnectorTableMetadata;
import org.pangrova.spi.connector.metadata.table.SchemaTableName;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.SupportsNamespaces;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.types.Types;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Catalog-side table resolution and listing for the Iceberg connector:
 * caches recently loaded tables, resolves live (refreshed) tables from
 * handles, and lists schemas/tables via the Iceberg catalog API.
 */
final class IcebergCatalogTables {

    private static final Logger LOG = LoggerFactory.getLogger(IcebergCatalogTables.class);

    private static final Duration TABLE_CACHE_TTL = Duration.ofSeconds(5);
    private static final int TABLE_CACHE_MAX_SIZE = 100;

    private final Catalog catalog;

    /**
     * Short-lived table metadata cache to avoid redundant catalog round-trips
     * during a single query planning cycle (F-CONNECTOR-09).
     */
    private final Cache<TableIdentifier, Table> tableCache;

    IcebergCatalogTables(Catalog catalog) {
        this.catalog = catalog;
        this.tableCache = Caffeine.newBuilder()
                .expireAfterWrite(TABLE_CACHE_TTL)
                .maximumSize(TABLE_CACHE_MAX_SIZE)
                .build();
    }

    /**
     * Resolves a live Iceberg Table from the catalog.
     * Always loads fresh from catalog AND calls refresh() to pick up
     * snapshots that may have been committed by another writer in the
     * same JVM but for which the catalog client still has cached
     * metadata.
     */
    Table resolveTable(IcebergTableHandle handle) {
        TableIdentifier id = TableIdentifier.of(handle.schemaName(), handle.tableName());
        Table table = catalog.loadTable(id);
        try {
            table.refresh();
        } catch (RuntimeException e) {
            LOG.warn("event=table_refresh_failed table={}.{} error={}",
                    handle.schemaName(), handle.tableName(), e.getMessage());
        }
        return table;
    }

    /** Loads a table handle, returning null when the table does not exist. */
    ConnectorTableHandle getTableHandle(SchemaTableName tableName) {
        TableIdentifier id = TableIdentifier.of(tableName.schema(), tableName.table());
        try {
            Table table = tableCache.get(id, k -> catalog.loadTable(k));
            return IcebergTableHandleFactory.fromTable(tableName.schema(), tableName.table(), table);
        } catch (org.apache.iceberg.exceptions.NoSuchTableException e) {
            LOG.debug("Table not found: {}", tableName);
            return null;
        }
    }

    /** Builds the connector table metadata for the given handle. */
    ConnectorTableMetadata getTableMetadata(IcebergTableHandle handle) {
        Table icebergTable = resolveTable(handle);
        var typedColumns = IcebergTypeMapper.toPangrovaColumns(
                icebergTable.schema(), icebergTable.properties());
        List<String> columnNames = typedColumns.stream()
                .map(ConnectorTableMetadata.Column::name)
                .toList();
        SchemaTableName name = new SchemaTableName(handle.schemaName(), handle.tableName());
        return new ConnectorTableMetadata(name, columnNames, typedColumns, Map.of());
    }

    /** Builds column handles from the table schema for the given handle. */
    Map<String, ColumnHandle> getColumnHandles(IcebergTableHandle handle) {
        Table icebergTable = resolveTable(handle);
        Map<String, ColumnHandle> handles = new LinkedHashMap<>();
        for (Types.NestedField field : icebergTable.schema().columns()) {
            PangrovaType pangrovaType = IcebergTypeMapper.toPangrovaType(field.type());
            handles.put(field.name(), new IcebergColumnHandle(field.fieldId(), field.name(), pangrovaType));
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
