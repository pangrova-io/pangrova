package org.pangrova.parser.api.ast;

import java.util.List;

/**
 * Represents a PIVOT clause that transposes rows into columns.
 *
 * <p>SQL syntax:
 * <pre>
 * table PIVOT (
 *   aggregate_function(value_column) [AS alias] [, ...]
 *   FOR pivot_column
 *   IN (value1 [AS alias1], value2 [AS alias2], ...)
 * ) [AS pivot_alias]
 * </pre>
 *
 * <p>Each {@link PivotAggregate} specifies an aggregate function applied to a value column.
 * The {@code forColumn} identifies which column's values become new column headers.
 * The {@code inValues} list specifies which values to pivot into columns.
 */
public record PivotClause(
        List<PivotAggregate> aggregates,
        Identifier forColumn,
        List<PivotValue> inValues) {

    public PivotClause {
        aggregates = List.copyOf(aggregates);
        inValues = List.copyOf(inValues);
    }

    /**
     * A single aggregate expression within a PIVOT clause.
     *
     * @param function the aggregate function name (e.g., SUM, AVG, COUNT)
     * @param argument the column or expression being aggregated
     * @param alias optional alias for the aggregated result
     */
    public record PivotAggregate(
            Identifier function,
            Expression argument,
            java.util.Optional<String> alias) {
    }

    /**
     * A value in the PIVOT IN list, representing a column header in the output.
     *
     * @param value the literal value to match in the pivot column
     * @param alias optional alias for the generated column name
     */
    public record PivotValue(
            Expression value,
            java.util.Optional<String> alias) {
    }
}
