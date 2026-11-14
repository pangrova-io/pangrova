package org.pangrova.planner.api.lineage;

import org.pangrova.governance.api.enums.ColumnDerivationKind;
import org.pangrova.common.api.annotation.Internal;

import java.util.List;

/**
 * Represents a single column derivation: an output column and the source columns it derives from.
 *
 * @param outputColumn  fully qualified output column name (e.g. "revenue")
 * @param sourceColumns source columns in "table.column" format that contribute to this output
 * @param expression    the expression type (DIRECT, TRANSFORM, AGGREGATE)
 */
@Internal
public record ColumnDerivation(
        String outputColumn,
        List<SourceColumn> sourceColumns,
        ColumnDerivationKind expression
) {
    public ColumnDerivation {
        sourceColumns = sourceColumns == null ? List.of() : List.copyOf(sourceColumns);
    }

    /**
     * A source column reference with table and column name.
     *
     * @param table  qualified table name (catalog.schema.table or just table)
     * @param column column name within the table
     */
    public record SourceColumn(String table, String column) {}
}
