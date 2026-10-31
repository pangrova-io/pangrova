package org.pangrova.connector.paimon;

import org.pangrova.common.exception.PangrovaErrorCode;
import org.pangrova.connector.paimon.handle.PaimonTableHandle;
import org.pangrova.connector.paimon.type.PaimonTypeMapper;
import org.pangrova.connector.spi.base.ConnectorException;
import org.pangrova.connector.spi.data.type.PangrovaType;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.schema.SchemaChange;
import org.apache.paimon.table.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * Schema DDL operations for Paimon tables: add/drop/rename/change column, create schema.
 * Extracted from PaimonMetadata to keep class size under 500 lines.
 */
final class PaimonDdlOperations {

    private static final Logger LOG = LoggerFactory.getLogger(PaimonDdlOperations.class);

    private PaimonDdlOperations() {}

    static void createSchema(Catalog catalog, String schemaName) {
        try {
            catalog.createDatabase(schemaName, false);
            LOG.info("event=paimon_database_created schema={}", schemaName);
        } catch (Catalog.DatabaseAlreadyExistException e) {
            LOG.debug("Paimon database '{}' already exists", schemaName);
        }
    }

    static void addColumn(Catalog catalog, PaimonTableHandle handle,
                          String columnName, PangrovaType columnType) {
        addColumn(catalog, handle, columnName, columnType, null);
    }

    static void addColumn(Catalog catalog, PaimonTableHandle handle,
                          String columnName, PangrovaType columnType, Object defaultValue) {
        Identifier id = toIdentifier(handle);
        org.apache.paimon.types.DataType paimonType =
                PaimonTypeMapper.toPaimonType(columnType);
        try {
            SchemaChange change = (defaultValue != null)
                    ? SchemaChange.addColumn(columnName, paimonType, String.valueOf(defaultValue))
                    : SchemaChange.addColumn(columnName, paimonType);
            catalog.alterTable(id, List.of(change), false);
            LOG.info("event=paimon_column_added column={} type={} default={} schema={} table={}",
                    columnName, columnType, defaultValue, handle.schemaName(), handle.tableName());
        } catch (Exception e) {
            throw new ConnectorException(PangrovaErrorCode.CONNECTOR_DDL_FAILED,
                    "Failed to add column '" + columnName + "' to " + id, e);
        }
    }

    static void dropColumn(Catalog catalog, PaimonTableHandle handle, String columnName) {
        Identifier id = toIdentifier(handle);
        try {
            catalog.alterTable(id, List.of(SchemaChange.dropColumn(columnName)), false);
            LOG.info("event=paimon_column_dropped column={} schema={} table={}",
                    columnName, handle.schemaName(), handle.tableName());
        } catch (Exception e) {
            throw new ConnectorException(PangrovaErrorCode.CONNECTOR_DDL_FAILED,
                    "Failed to drop column '" + columnName + "' from " + id, e);
        }
    }

    static void renameColumn(Catalog catalog, PaimonTableHandle handle,
                             String oldName, String newName) {
        Identifier id = toIdentifier(handle);
        try {
            catalog.alterTable(id, List.of(SchemaChange.renameColumn(oldName, newName)), false);
            LOG.info("event=paimon_column_renamed oldName={} newName={} schema={} table={}",
                    oldName, newName, handle.schemaName(), handle.tableName());
        } catch (Exception e) {
            throw new ConnectorException(PangrovaErrorCode.CONNECTOR_DDL_FAILED,
                    "Failed to rename column in " + id, e);
        }
    }

    static void changeColumnType(Catalog catalog, PaimonTableHandle handle,
                                 String columnName, String newType) {
        Identifier id = toIdentifier(handle);
        org.apache.paimon.types.DataType paimonType =
                PaimonTypeMapper.toPaimonType(PangrovaType.fromSqlTypeName(newType));
        try {
            catalog.alterTable(id, List.of(SchemaChange.updateColumnType(columnName, paimonType)), false);
            LOG.info("event=paimon_column_type_changed column={} newType={} schema={} table={}",
                    columnName, newType, handle.schemaName(), handle.tableName());
        } catch (Exception e) {
            throw new ConnectorException(PangrovaErrorCode.CONNECTOR_DDL_FAILED,
                    "Failed to change column type in " + id, e);
        }
    }

    private static Identifier toIdentifier(PaimonTableHandle handle) {
        return Identifier.create(handle.schemaName(), handle.tableName());
    }

    /**
     * Logs a warning when a PK table has partition keys that differ from
     * primary keys but cross-partition-upsert is not enabled. Paimon will
     * reject cross-partition upserts at write time without this option.
     */
    static void warnCrossPartitionUpdateIfNeeded(Table table, PaimonTableHandle handle) {
        List<String> pk = table.primaryKeys();
        List<String> partitionKeys = table.partitionKeys();
        if (pk.isEmpty() || partitionKeys.isEmpty()) {
            return;
        }
        if (pk.containsAll(partitionKeys)) {
            return;
        }
        String crossPartition = table.options().getOrDefault("cross-partition-upsert", "false");
        if (!"true".equalsIgnoreCase(crossPartition)) {
            LOG.warn("event=cross_partition_upsert_disabled table={}.{} "
                            + "partitionKeys={} primaryKeys={} "
                            + "hint=set_table_property_cross-partition-upsert_to_true",
                    handle.schemaName(), handle.tableName(), partitionKeys, pk);
        }
    }
}
