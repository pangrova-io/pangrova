package org.pangrova.planner.lineage;

import java.util.List;

/**
 * Represents a single column derivation: an output column and the source columns it derives from.
 *
 * @param outputColumn  fully qualified output column name (e.g. "revenue")
 * @param sourceColumns source columns in "table.column" format that contribute to this output
 * @param expression    the expression type (DIRECT, TRANSFORM, AGGREGATE)
 */
public record ColumnDerivation(
        String outputColumn,
        List<SourceColumn> sourceColumns,
        DerivationType expression
) {
    public ColumnDerivation {
        sourceColumns = sourceColumns == null ? List.of() : List.copyOf(sourceColumns);
    }

    public enum DerivationType {
        /** Direct column reference (SELECT col FROM t) */
        DIRECT,
        /** Expression-based transformation (SELECT col * 2 FROM t) */
        TRANSFORM,
        /** Aggregate function (SELECT SUM(col) FROM t) */
        AGGREGATE
    }

    /**
     * A source column reference with table and column name.
     *
     * @param table  qualified table name (catalog.schema.table or just table)
     * @param column column name within the table
     */
    public record SourceColumn(String table, String column) {}
}
