package org.pangrova.connector.spi.index.predicate;

import org.pangrova.common.api.annotation.Experimental;

/**
 * Predicate for SQL LIKE pattern matching, evaluated against prefix or
 * N-gram bloom filter indexes.
 *
 * @param column  target column name (must not be blank)
 * @param pattern the LIKE pattern (e.g. "abc%", "%xyz%")
 */
@Experimental
public record LikePattern(String column, String pattern) implements IndexPredicate {

    public LikePattern {
        if (column == null || column.isBlank()) {
            throw new IllegalArgumentException("column must not be blank");
        }
        if (pattern == null || pattern.isBlank()) {
            throw new IllegalArgumentException("pattern must not be blank");
        }
    }
}
