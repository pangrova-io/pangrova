package org.pangrova.parser.api.ast;

import org.pangrova.parser.api.enums.NullOrdering;
import org.pangrova.parser.api.enums.SortDirection;

import java.util.Optional;

/**
 * Represents an ORDER BY sort item with expression, optional direction, and optional null ordering.
 */
public record SortItem(Expression expression, Optional<SortDirection> direction, Optional<NullOrdering> nullOrdering) {

    public SortItem(Expression expression) {
        this(expression, Optional.empty(), Optional.empty());
    }

    public SortItem(Expression expression, Optional<SortDirection> direction) {
        this(expression, direction, Optional.empty());
    }
}
