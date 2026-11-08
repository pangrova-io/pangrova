package org.pangrova.materialization.domain;

/**
 * Physical table that stores one generation.
 *
 * <p>The table must live in the internal materialization schema and carry a
 * well-formed candidate name. Construction fails otherwise, so an invalid
 * physical location cannot reach persistence or execution.
 *
 * @param catalog catalog name
 * @param schema internal schema name
 * @param table candidate table name
 */
public record PhysicalTableRef(String catalog, String schema, String table) {

    public PhysicalTableRef {
        catalog = Require.text(catalog, "catalog");
        schema = Require.text(schema, "schema");
        table = Require.text(table, "table");
        if (!InternalNamespace.isReservedSchema(schema)) {
            throw new IllegalArgumentException("physical table must use the internal schema: " + schema);
        }
        if (!InternalNamespace.isCandidateTable(table)) {
            throw new IllegalArgumentException("physical table must match __pg_mat_<32 hex>: " + table);
        }
    }

    /**
     * Returns the dot-qualified physical table name.
     *
     * @return qualified name
     */
    public String qualifiedName() {
        return catalog + "." + schema + "." + table;
    }

    /**
     * Returns whether this reference names the same physical table.
     *
     * @param other other reference
     * @return true when catalog, schema, and table match
     */
    public boolean sameTable(PhysicalTableRef other) {
        return other != null
                && catalog.equals(other.catalog)
                && schema.equals(other.schema)
                && table.equals(other.table);
    }
}
