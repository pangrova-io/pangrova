package org.pangrova.spi.connector.index.predicate;

import org.pangrova.common.api.annotation.Experimental;

/**
 * Predicate matching rows where the column value falls within a range.
 *
 * <p>Either bound may be {@code null} to represent an open-ended range.
 * At least one bound must be non-null.
 *
 * @param column       target column name (must not be blank)
 * @param lower        lower bound (nullable for open-ended)
 * @param upper        upper bound (nullable for open-ended)
 * @param lowerInclusive whether the lower bound is inclusive
 * @param upperInclusive whether the upper bound is inclusive
 */
@Experimental
public record RangeQuery(
        String column,
        Comparable<?> lower,
        Comparable<?> upper,
        boolean lowerInclusive,
        boolean upperInclusive
) implements IndexPredicate {

    public RangeQuery {
        if (column == null || column.isBlank()) {
            throw new IllegalArgumentException("column must not be blank");
        }
        if (lower == null && upper == null) {
            throw new IllegalArgumentException("at least one bound must be non-null");
        }
    }
}
