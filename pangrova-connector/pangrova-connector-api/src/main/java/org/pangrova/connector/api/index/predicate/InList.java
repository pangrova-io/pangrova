package org.pangrova.connector.api.index.predicate;

import java.util.List;
import java.util.Objects;

import org.pangrova.common.api.annotation.Experimental;

/**
 * Predicate matching rows where the column value is in the specified list.
 *
 * @param column target column name (must not be blank)
 * @param values non-empty list of values to match against (defensively copied)
 */
@Experimental
public record InList(String column, List<Comparable<?>> values) implements IndexPredicate {

    public InList {
        if (column == null || column.isBlank()) {
            throw new IllegalArgumentException("column must not be blank");
        }
        Objects.requireNonNull(values, "values must not be null");
        if (values.isEmpty()) {
            throw new IllegalArgumentException("values must not be empty");
        }
        values = List.copyOf(values);
    }
}
