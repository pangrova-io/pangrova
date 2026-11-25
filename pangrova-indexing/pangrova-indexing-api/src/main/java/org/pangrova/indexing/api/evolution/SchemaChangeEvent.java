package org.pangrova.indexing.api.evolution;

import org.pangrova.common.api.annotation.Experimental;
import org.pangrova.indexing.api.enums.SchemaChangeType;

/**
 * Immutable event describing a schema change that may affect indexes.
 *
 * @param type           the type of schema change
 * @param tenantId      tenant owning affected indexes
 * @param catalogName    catalog containing the table
 * @param schemaName     schema containing the table before the change
 * @param tableName      table name after the change
 * @param oldTableName   previous table name (RENAME_TABLE only, null otherwise)
 * @param oldColumnName  previous column name (RENAME_COLUMN only, null otherwise)
 * @param newColumnName  new column name (RENAME_COLUMN/ADD_COLUMN, null otherwise)
 * @param oldColumnType  previous column type (MODIFY_COLUMN_TYPE only, null otherwise)
 * @param newColumnType  new column type (MODIFY_COLUMN_TYPE/ADD_COLUMN, null otherwise)
 * @param newSchemaName  target schema for RENAME_TABLE, null otherwise
 */
@Experimental
public record SchemaChangeEvent(
        SchemaChangeType type,
        String tenantId,
        String catalogName,
        String schemaName,
        String tableName,
        String oldTableName,
        String oldColumnName,
        String newColumnName,
        String oldColumnType,
        String newColumnType,
        String newSchemaName) {

    public SchemaChangeEvent {
        if (type == null) {
            throw new IllegalArgumentException("type required");
        }
        if (tenantId == null || tenantId.isBlank()) {
            throw new IllegalArgumentException("tenantId required");
        }
        if (catalogName == null || catalogName.isBlank()) {
            throw new IllegalArgumentException("catalogName required");
        }
        if (schemaName == null || schemaName.isBlank()) {
            throw new IllegalArgumentException("schemaName required");
        }
        if (tableName == null || tableName.isBlank()) {
            throw new IllegalArgumentException("tableName required");
        }
    }

    /** Factory for DROP_COLUMN events. */
    public static SchemaChangeEvent dropColumn(
            String tenantId, String catalog, String schema, String table, String column) {
        return new SchemaChangeEvent(
                SchemaChangeType.DROP_COLUMN, tenantId, catalog, schema, table,
                null, column, null, null, null, null);
    }

    /** Factory for RENAME_COLUMN events. */
    public static SchemaChangeEvent renameColumn(
            String tenantId, String catalog, String schema, String table,
            String oldColumn, String newColumn) {
        return new SchemaChangeEvent(
                SchemaChangeType.RENAME_COLUMN, tenantId, catalog, schema, table,
                null, oldColumn, newColumn, null, null, null);
    }

    /** Factory for MODIFY_COLUMN_TYPE events. */
    public static SchemaChangeEvent modifyColumnType(
            String tenantId, String catalog, String schema, String table,
            String column, String oldType, String newType) {
        return new SchemaChangeEvent(
                SchemaChangeType.MODIFY_COLUMN_TYPE, tenantId, catalog, schema, table,
                null, column, null, oldType, newType, null);
    }

    /** Factory for DROP_TABLE events. */
    public static SchemaChangeEvent dropTable(
            String tenantId, String catalog, String schema, String table) {
        return new SchemaChangeEvent(
                SchemaChangeType.DROP_TABLE, tenantId, catalog, schema, table,
                null, null, null, null, null, null);
    }

    /** Factory for RENAME_TABLE events. */
    public static SchemaChangeEvent renameTable(
            String tenantId, String catalog, String oldSchema, String oldTable,
            String newSchema, String newTable) {
        return new SchemaChangeEvent(
                SchemaChangeType.RENAME_TABLE, tenantId, catalog, oldSchema, newTable,
                oldTable, null, null, null, null, newSchema);
    }

    /** Factory for ADD_COLUMN events. */
    public static SchemaChangeEvent addColumn(
            String tenantId, String catalog, String schema, String table,
            String column, String columnType) {
        return new SchemaChangeEvent(
                SchemaChangeType.ADD_COLUMN, tenantId, catalog, schema, table,
                null, null, column, null, columnType, null);
    }
}
