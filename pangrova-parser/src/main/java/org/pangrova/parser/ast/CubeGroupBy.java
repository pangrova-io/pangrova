package org.pangrova.parser.ast;

import java.util.List;

/**
 * Represents a {@code CUBE} clause in GROUP BY.
 *
 * <p>{@code CUBE(a, b)} is syntactic sugar for
 * {@code GROUPING SETS ((a, b), (a), (b), ())} — all 2^N subsets
 * of the specified columns.</p>
 *
 * @param columns the columns to cube over
 */
public record CubeGroupBy(List<Expression> columns) implements GroupByItem {

    public CubeGroupBy {
        if (columns == null) {
            throw new IllegalArgumentException("columns must not be null");
        }
        columns = List.copyOf(columns);
    }
}
