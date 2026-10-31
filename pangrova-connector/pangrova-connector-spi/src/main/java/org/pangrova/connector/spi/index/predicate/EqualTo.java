package org.pangrova.connector.spi.index.predicate;

import java.util.Objects;

import org.pangrova.common.api.annotation.Experimental;

/**
 * Predicate matching rows where the column value equals the specified value.
 *
 * @param column target column name (must not be blank)
 * @param value  the value to match against
 */
@Experimental
public record EqualTo(String column, Comparable<?> value) implements IndexPredicate {

    public EqualTo {
        if (column == null || column.isBlank()) {
            throw new IllegalArgumentException("column must not be blank");
        }
        Objects.requireNonNull(value, "value must not be null");
    }
}
