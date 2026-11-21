package org.pangrova.indexing.enums;

/**
 * Types of schema changes that may affect index validity.
 */
public enum SchemaChangeType {
    ADD_COLUMN,
    DROP_COLUMN,
    RENAME_COLUMN,
    MODIFY_COLUMN_TYPE,
    DROP_TABLE,
    RENAME_TABLE
}
