package org.pangrova.parser.ast.expression;

import java.util.List;

import org.pangrova.parser.ast.Expression;
import org.pangrova.parser.ast.ExpressionType;
import org.pangrova.parser.ast.Identifier;
import org.pangrova.parser.ast.WindowSpec;

/**
 * Represents a function call with a window specification (OVER clause).
 */
public record WindowedFunctionCall(
        Identifier name,
        List<Expression> arguments,
        boolean distinct,
        WindowSpec window) implements Expression {

    public WindowedFunctionCall {
        arguments = List.copyOf(arguments);
    }

    /** Convenience constructor without distinct flag (backward compatible). */
    public WindowedFunctionCall(Identifier name, List<Expression> arguments, WindowSpec window) {
        this(name, arguments, false, window);
    }

    @Override
    public ExpressionType type() {
        return ExpressionType.WINDOWED_FUNCTION_CALL;
    }
}
