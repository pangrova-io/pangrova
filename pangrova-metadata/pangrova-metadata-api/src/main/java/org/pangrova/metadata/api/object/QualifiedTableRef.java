package org.pangrova.metadata.api.object;

import static org.pangrova.common.validation.Preconditions.checkNotNull;

/**
 * Three-part table reference: catalog.schema.table.
 */
public record QualifiedTableRef(String catalog, String schema, String table) {

    public QualifiedTableRef {
        checkNotNull(catalog, "catalog must not be null");
        checkNotNull(schema, "schema must not be null");
        checkNotNull(table, "table must not be null");
    }
}
