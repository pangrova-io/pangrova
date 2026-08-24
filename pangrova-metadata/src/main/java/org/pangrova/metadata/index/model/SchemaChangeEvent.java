package org.pangrova.metadata.index.model;

/**
 * Immutable event describing a schema change that may affect indexes.
 *
 * @param type           the type of schema change
 * @param catalogName    catalog containing the table
 * @param schemaName     schema containing the table
 * @param tableName      current table name
 * @param oldTableName   previous table name (RENAME_TABLE only, null otherwise)
 * @param oldColumnName  previous column name (RENAME_COLUMN only, null otherwise)
 * @param newColumnName  new column name (RENAME_COLUMN/ADD_COLUMN, null otherwise)
 * @param oldColumnType  previous column type (MODIFY_COLUMN_TYPE only, null otherwise)
 * @param newColumnType  new column type (MODIFY_COLUMN_TYPE/ADD_COLUMN, null otherwise)
 */
public record SchemaChangeEvent(
        SchemaChangeType type,
        String catalogName,
        String schemaName,
        String tableName,
        String oldTableName,
        String oldColumnName,
        String newColumnName,
        String oldColumnType,
        String newColumnType) {

    public SchemaChangeEvent {
        if (type == null) {
            throw new IllegalArgumentException("type required");
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
            String catalog, String schema, String table, String column) {
        return new SchemaChangeEvent(
                SchemaChangeType.DROP_COLUMN, catalog, schema, table,
                null, column, null, null, null);
    }

    /** Factory for RENAME_COLUMN events. */
    public static SchemaChangeEvent renameColumn(
            String catalog, String schema, String table,
            String oldColumn, String newColumn) {
        return new SchemaChangeEvent(
                SchemaChangeType.RENAME_COLUMN, catalog, schema, table,
                null, oldColumn, newColumn, null, null);
    }

    /** Factory for MODIFY_COLUMN_TYPE events. */
    public static SchemaChangeEvent modifyColumnType(
            String catalog, String schema, String table,
            String column, String oldType, String newType) {
        return new SchemaChangeEvent(
                SchemaChangeType.MODIFY_COLUMN_TYPE, catalog, schema, table,
                null, column, null, oldType, newType);
    }

    /** Factory for DROP_TABLE events. */
    public static SchemaChangeEvent dropTable(
            String catalog, String schema, String table) {
        return new SchemaChangeEvent(
                SchemaChangeType.DROP_TABLE, catalog, schema, table,
                null, null, null, null, null);
    }

    /** Factory for RENAME_TABLE events. */
    public static SchemaChangeEvent renameTable(
            String catalog, String schema, String oldTable, String newTable) {
        return new SchemaChangeEvent(
                SchemaChangeType.RENAME_TABLE, catalog, schema, newTable,
                oldTable, null, null, null, null);
    }

    /** Factory for ADD_COLUMN events. */
    public static SchemaChangeEvent addColumn(
            String catalog, String schema, String table,
            String column, String columnType) {
        return new SchemaChangeEvent(
                SchemaChangeType.ADD_COLUMN, catalog, schema, table,
                null, null, column, null, columnType);
    }
}
