package org.pangrova.connector.api.clustering;

import org.pangrova.common.api.annotation.Experimental;

/**
 * Fully qualified table reference for clustering operations.
 *
 * @param catalogName catalog identifier
 * @param schemaName  schema identifier
 * @param tableName   table identifier
 */
@Experimental
public record ClusteringTableRef(
        String catalogName,
        String schemaName,
        String tableName) {

    public ClusteringTableRef {
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

    @Override
    public String toString() {
        return catalogName + "." + schemaName + "." + tableName;
    }
}
