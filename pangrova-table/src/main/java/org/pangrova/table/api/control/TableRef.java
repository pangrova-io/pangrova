package org.pangrova.table.api.control;

import java.util.Objects;

/**
 * Table-owned catalog.schema.table identity used by the control plane.
 *
 * <p>Metadata and Coordinator adapters may translate this reference into their
 * own view of a qualified name; the table owner never depends on those modules.</p>
 */
public record TableRef(String catalog, String schema, String table) {

    public TableRef {
        Objects.requireNonNull(catalog, "catalog");
        Objects.requireNonNull(schema, "schema");
        Objects.requireNonNull(table, "table");
    }

    public String qualifiedName() {
        return catalog + "." + schema + "." + table;
    }
}
