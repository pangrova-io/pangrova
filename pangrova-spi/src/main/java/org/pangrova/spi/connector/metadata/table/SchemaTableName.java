package org.pangrova.spi.connector.metadata.table;

import org.pangrova.spi.annotation.Experimental;

import static org.pangrova.common.util.Preconditions.checkNotNull;

/**
 * Immutable value type representing a fully qualified table name within a schema.
 *
 * @param schema the schema name, must not be null
 * @param table  the table name, must not be null
 */
@Experimental
public record SchemaTableName(String schema, String table) {

    /**
     * Creates a new schema-qualified table name.
     *
     * @param schema the schema name
     * @param table  the table name
     * @throws NullPointerException if schema or table is null
     */
    public SchemaTableName {
        checkNotNull(schema, "schema must not be null");
        checkNotNull(table, "table must not be null");
    }

    @Override
    public String toString() {
        return schema + "." + table;
    }
}
