package org.pangrova.planner;

import org.pangrova.spi.connector.metadata.Constraint;
import org.pangrova.spi.connector.metadata.table.ConnectorReadBinding;

import java.util.List;
import java.util.Optional;

/**
 * Calcite-free descriptor of a table scan extracted from a query plan.
 *
 * <p>Used by SplitFetcher and IndexAwareSplitFetcher to enumerate connector
 * splits without requiring Calcite types in the coordinator module. All Calcite
 * tree-walking and predicate extraction is encapsulated in
 * {@link PlanSplitSourceExtractor}, which produces instances of this record.</p>
 *
 * @param catalogName   catalog name (or "default" if not qualified)
 * @param schemaName    schema name
 * @param tableName     table name
 * @param constraint    predicate constraint extracted from the plan (may be {@link Constraint#EMPTY})
 * @param tableColumns  column names visible in this table scan
 * @param readBinding   selected connector source shared by planning and split enumeration;
 *                      empty only for descriptors without a metadata-backed planning table
 */
public record SplitSourceDescriptor(
        String catalogName,
        String schemaName,
        String tableName,
        Constraint constraint,
        List<String> tableColumns,
        Optional<ConnectorReadBinding> readBinding) {

    public SplitSourceDescriptor {
        tableColumns = tableColumns != null ? List.copyOf(tableColumns) : List.of();
        java.util.Objects.requireNonNull(readBinding, "readBinding");
    }

    /**
     * Returns the fully-qualified table name (catalog.schema.table).
     */
    public String qualifiedName() {
        return catalogName + "." + schemaName + "." + tableName;
    }
}
