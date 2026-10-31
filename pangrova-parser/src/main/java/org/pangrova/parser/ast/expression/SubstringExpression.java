package org.pangrova.parser.ast.expression;

import org.pangrova.parser.ast.Expression;
import org.pangrova.parser.ast.ExpressionType;

import java.util.Objects;

/**
 * Represents SUBSTRING(expr FROM start FOR length).
 */
public record SubstringExpression(Expression source, Expression start, Expression length) implements Expression {
    public SubstringExpression {
        Objects.requireNonNull(source, "substring source must not be null");
        Objects.requireNonNull(start, "substring start must not be null");
        Objects.requireNonNull(length, "substring length must not be null");
    }

    @Override
    public ExpressionType type() {
        return ExpressionType.SUBSTRING;
    }
}
