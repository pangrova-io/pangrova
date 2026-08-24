package org.pangrova.parser.ast;

import java.util.Optional;

/**
 * Represents an ORDER BY sort item with expression, optional direction, and optional null ordering.
 */
public record SortItem(Expression expression, Optional<SortDirection> direction, Optional<NullOrdering> nullOrdering) {

    public enum SortDirection {
        ASC,
        DESC
    }

    public enum NullOrdering {
        FIRST,
        LAST
    }

    public SortItem(Expression expression) {
        this(expression, Optional.empty(), Optional.empty());
    }

    public SortItem(Expression expression, Optional<SortDirection> direction) {
        this(expression, direction, Optional.empty());
    }
}
