package org.pangrova.connector.spi.index.predicate;

import org.pangrova.common.api.annotation.Experimental;

/**
 * Predicate matching rows where the column value is null.
 *
 * @param column target column name (must not be blank)
 */
@Experimental
public record IsNull(String column) implements IndexPredicate {

    public IsNull {
        if (column == null || column.isBlank()) {
            throw new IllegalArgumentException("column must not be blank");
        }
    }
}
