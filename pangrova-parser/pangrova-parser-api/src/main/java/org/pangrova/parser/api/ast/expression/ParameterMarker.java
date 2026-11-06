package org.pangrova.parser.api.ast.expression;

import org.pangrova.parser.api.ast.Expression;
import org.pangrova.parser.api.ast.ExpressionType;

/**
 * Represents a parameter marker ({@code ?}) in a prepared statement.
 *
 * <p>Parameter markers are positional placeholders that are bound to actual
 * values at execution time. They appear in SQL as a single question mark
 * character and are numbered sequentially starting from 1.</p>
 */
public record ParameterMarker() implements Expression {

    @Override
    public ExpressionType type() {
        return ExpressionType.PARAMETER_MARKER;
    }
}
