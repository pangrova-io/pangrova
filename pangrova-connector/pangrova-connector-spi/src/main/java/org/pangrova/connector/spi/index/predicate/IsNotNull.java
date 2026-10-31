package org.pangrova.connector.spi.index.predicate;

import org.pangrova.common.api.annotation.Experimental;

/**
 * Predicate matching rows where the column value is not null.
 *
 * @param column target column name (must not be blank)
 */
@Experimental
public record IsNotNull(String column) implements IndexPredicate {

    public IsNotNull {
        if (column == null || column.isBlank()) {
            throw new IllegalArgumentException("column must not be blank");
        }
    }
}
