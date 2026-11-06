package org.pangrova.planner.core.incremental;

import java.util.Objects;

/**
 * Describes how a single aggregation column should be merged during
 * incremental DT refresh.
 *
 * @param sourceOperator the original SQL aggregation operator (e.g. "SUM", "COUNT")
 * @param mergeOperator  the merge strategy to combine old + delta values
 */
public record ColumnMergeStrategy(
        String sourceOperator,
        MergeOperator mergeOperator
) {

    public ColumnMergeStrategy {
        Objects.requireNonNull(sourceOperator, "sourceOperator must not be null");
        Objects.requireNonNull(mergeOperator, "mergeOperator must not be null");
    }
}
