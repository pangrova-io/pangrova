package org.pangrova.connector.api.materialization;

import org.pangrova.common.api.annotation.Stable;

import static org.pangrova.common.util.Preconditions.requireText;

/**
 * Fully qualified identity of a table read by a materialization definition.
 *
 * <p>The connector identifier is part of the identity because the same catalog
 * and table name may resolve to different physical sources across connectors.
 *
 * @param connectorId stable connector identifier, for example {@code iceberg}
 * @param catalog catalog name
 * @param schema schema name
 * @param table table name
 */
@Stable(since = "1.0")
public record SourceRef(String connectorId, String catalog, String schema, String table) {

    public SourceRef {
        connectorId = requireText(connectorId, "connectorId");
        catalog = requireText(catalog, "catalog");
        schema = requireText(schema, "schema");
        table = requireText(table, "table");
    }

    /**
     * Returns the canonical, collision-free representation used in source vectors.
     *
     * @return canonical source identity
     */
    public String canonicalName() {
        return connectorId + ":" + catalog + "." + schema + "." + table;
    }
}
