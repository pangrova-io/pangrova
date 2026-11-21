package org.pangrova.parser.api.ast.expression;

import java.util.List;
import java.util.Optional;

import org.pangrova.parser.api.ast.Expression;
import org.pangrova.parser.api.enums.ExpressionType;
import org.pangrova.parser.api.ast.Identifier;

/**
 * Represents a function call expression (e.g., COUNT(*), SUM(x), COUNT(DISTINCT x)).
 *
 * <p>{@code filter} carries the SQL:2003 aggregate filter clause
 * ({@code agg(...) FILTER (WHERE condition)}); empty for non-filtered calls.</p>
 */
public record FunctionCall(Identifier name, List<Expression> arguments, boolean distinct,
        Optional<Expression> filter) implements Expression {

    public FunctionCall {
        arguments = List.copyOf(arguments);
    }

    /** Convenience constructor without a FILTER clause. */
    public FunctionCall(Identifier name, List<Expression> arguments, boolean distinct) {
        this(name, arguments, distinct, Optional.empty());
    }

    /** Convenience constructor without distinct flag (backward compatible). */
    public FunctionCall(Identifier name, List<Expression> arguments) {
        this(name, arguments, false, Optional.empty());
    }

    @Override
    public ExpressionType type() {
        return ExpressionType.FUNCTION_CALL;
    }
}
