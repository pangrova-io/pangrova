package org.pangrova.parser.api.ast.expression;

import java.util.Optional;

import org.pangrova.parser.api.ast.Expression;
import org.pangrova.parser.api.enums.ExpressionType;
import org.pangrova.parser.api.ast.Identifier;
import org.pangrova.parser.api.ast.WindowSpec;

/**
 * Represents a function call with star argument, e.g. COUNT(*).
 * Optionally includes a window specification (OVER clause).
 *
 * <p>{@code filter} carries the SQL:2003 aggregate filter clause
 * ({@code COUNT(*) FILTER (WHERE condition)}); empty for non-filtered calls.</p>
 */
public record StarFunctionCall(
        Identifier name,
        Optional<WindowSpec> window,
        Optional<Expression> filter) implements Expression {

    /** Convenience constructor without a FILTER clause (backward compatible). */
    public StarFunctionCall(Identifier name, Optional<WindowSpec> window) {
        this(name, window, Optional.empty());
    }

    @Override
    public ExpressionType type() {
        return ExpressionType.STAR_FUNCTION_CALL;
    }
}
