package org.pangrova.governance.api.lineage;

import org.pangrova.governance.api.enums.ColumnDerivationKind;

import java.util.List;

/**
 * Reference to a dataset (table) with optional column lineage.
 */
public record DatasetRef(
    String namespace,
    String name,
    List<ColumnLineage> columnLineage
) {
    public DatasetRef {
        columnLineage = columnLineage == null ? List.of() : List.copyOf(columnLineage);
    }

    /**
     * Structured reference to a source column, avoiding fragile dot-separated string parsing.
     *
     * @param namespace OpenLineage namespace (e.g. "pangrova")
     * @param dataset   source dataset/table name (may contain dots)
     * @param column    column name within the dataset
     */
    public record SourceColumnRef(String namespace, String dataset, String column) {}

    /**
     * Maps an output column to its contributing source columns.
     *
     * @param outputColumn  name of the output column
     * @param sourceColumns structured references to source columns
     */
    public record ColumnLineage(
        String outputColumn,
        List<SourceColumnRef> sourceColumns,
        ColumnDerivationKind derivation
    ) {
        /** A two-argument direct column reference has identity semantics. */
        public ColumnLineage(String outputColumn, List<SourceColumnRef> sourceColumns) {
            this(outputColumn, sourceColumns,
                    ColumnDerivationKind.DIRECT);
        }

        public ColumnLineage {
            sourceColumns = sourceColumns == null ? List.of() : List.copyOf(sourceColumns);
            java.util.Objects.requireNonNull(derivation, "derivation");
        }
    }
}
