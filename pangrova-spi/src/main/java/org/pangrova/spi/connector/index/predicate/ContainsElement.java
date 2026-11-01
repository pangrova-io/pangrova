package org.pangrova.spi.connector.index.predicate;

import java.util.List;
import java.util.Objects;

import org.pangrova.common.api.annotation.Experimental;

/**
 * Predicate for element containment queries evaluated against a GIN index.
 *
 * @param column   target column name (must not be blank)
 * @param elements non-empty list of elements to check (defensively copied)
 * @param mode     containment mode (ANY or ALL)
 */
@Experimental
public record ContainsElement(
        String column,
        List<Comparable<?>> elements,
        ContainmentMode mode
) implements IndexPredicate {

    public ContainsElement {
        if (column == null || column.isBlank()) {
            throw new IllegalArgumentException("column must not be blank");
        }
        Objects.requireNonNull(elements, "elements must not be null");
        if (elements.isEmpty()) {
            throw new IllegalArgumentException("elements must not be empty");
        }
        elements = List.copyOf(elements);
        Objects.requireNonNull(mode, "mode must not be null");
    }
}
