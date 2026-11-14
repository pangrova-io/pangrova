package org.pangrova.parser.api.ast;

import org.pangrova.parser.api.enums.UnpivotNullHandling;

import java.util.List;
import java.util.Optional;

/**
 * Represents an UNPIVOT clause that transposes columns into rows.
 *
 * <p>SQL syntax:
 * <pre>
 * table UNPIVOT [INCLUDE NULLS | EXCLUDE NULLS] (
 *   value_column
 *   FOR name_column
 *   IN (col1 [AS 'label1'], col2 [AS 'label2'], ...)
 * ) [AS unpivot_alias]
 * </pre>
 *
 * <p>The {@code valueColumn} receives the cell values from the unpivoted columns.
 * The {@code nameColumn} receives the column name (or alias) identifying which source column
 * produced each row. The {@code sourceColumns} list specifies which columns to unpivot.
 */
public record UnpivotClause(
        Identifier valueColumn,
        Identifier nameColumn,
        List<UnpivotColumn> sourceColumns,
        UnpivotNullHandling nullHandling) {

    public UnpivotClause {
        sourceColumns = List.copyOf(sourceColumns);
    }

    /**
     * A source column in the UNPIVOT IN list.
     *
     * @param column the column identifier to unpivot
     * @param alias optional label for the name column value (defaults to column name)
     */
    public record UnpivotColumn(
            Identifier column,
            Optional<String> alias) {
    }
}
