package org.pangrova.connector.spi.metadata;

import java.util.List;

import org.pangrova.common.api.annotation.Experimental;

/**
 * Connector-agnostic filter expression for row-level DELETE operations.
 *
 * <p>Represents the WHERE clause of a DELETE statement in a form that
 * connectors can translate to their native filter API (e.g., Iceberg
 * Expressions, Paimon Predicates). Kept intentionally simple to cover
 * common DELETE patterns without coupling SPI to any specific connector.</p>
 */
@Experimental
public sealed interface DeleteFilter {

    /** Deletes all rows (DELETE without WHERE). */
    record All() implements DeleteFilter {}

    /** Column equals a literal value: column = value. */
    record Equal(String column, Object value) implements DeleteFilter {}

    /** Column does not equal a literal value: column != value. */
    record NotEqual(String column, Object value) implements DeleteFilter {}

    /** Column is less than a literal value: column < value. */
    record LessThan(String column, Object value) implements DeleteFilter {}

    /** Column is less than or equal to a literal value: column <= value. */
    record LessThanOrEqual(String column, Object value) implements DeleteFilter {}

    /** Column is greater than a literal value: column > value. */
    record GreaterThan(String column, Object value) implements DeleteFilter {}

    /** Column is greater than or equal to a literal value: column >= value. */
    record GreaterThanOrEqual(String column, Object value) implements DeleteFilter {}

    /** Logical AND of multiple filters. */
    record And(List<DeleteFilter> filters) implements DeleteFilter {
        public And {
            filters = List.copyOf(filters);
        }
    }

    /** Logical OR of multiple filters. */
    record Or(List<DeleteFilter> filters) implements DeleteFilter {
        public Or {
            filters = List.copyOf(filters);
        }
    }

    /** Logical NOT of a filter. */
    record Not(DeleteFilter filter) implements DeleteFilter {}

    /** Column is NULL. */
    record IsNull(String column) implements DeleteFilter {}

    /** Column is NOT NULL. */
    record IsNotNull(String column) implements DeleteFilter {}

    /** Column value is in a list: column IN (v1, v2, ...). */
    record In(String column, List<Object> values) implements DeleteFilter {
        public In {
            values = List.copyOf(values);
        }
    }
}
