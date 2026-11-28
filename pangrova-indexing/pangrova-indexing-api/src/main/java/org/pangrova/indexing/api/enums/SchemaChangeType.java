package org.pangrova.indexing.api.enums;

import org.pangrova.common.api.annotation.Experimental;

/**
 * Types of schema changes that may affect index validity.
 */
@Experimental
public enum SchemaChangeType {
    ADD_COLUMN,
    DROP_COLUMN,
    RENAME_COLUMN,
    MODIFY_COLUMN_TYPE,
    DROP_TABLE,
    RENAME_TABLE,
    SCHEMA_MISMATCH
}
