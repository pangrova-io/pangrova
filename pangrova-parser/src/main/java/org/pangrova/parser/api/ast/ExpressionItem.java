package org.pangrova.parser.api.ast;

import java.util.Optional;

/**
 * Represents a select item with an expression and optional alias.
 */
public record ExpressionItem(Expression expression, Optional<String> alias) implements SelectItem {

    public ExpressionItem(Expression expression) {
        this(expression, Optional.empty());
    }
}
