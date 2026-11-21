package org.pangrova.parser.api.ast;

import java.util.List;

/**
 * Represents a {@code ROLLUP} clause in GROUP BY.
 *
 * <p>{@code ROLLUP(a, b)} is syntactic sugar for
 * {@code GROUPING SETS ((a, b), (a), ())} — a hierarchical
 * sequence of N+1 grouping sets, progressively removing
 * columns from right to left.</p>
 *
 * @param columns the columns to roll up over
 */
public record RollupGroupBy(List<Expression> columns) implements GroupByItem {

    public RollupGroupBy {
        if (columns == null) {
            throw new IllegalArgumentException("columns must not be null");
        }
        columns = List.copyOf(columns);
    }
}
