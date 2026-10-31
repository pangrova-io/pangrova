package org.pangrova.parser.ast.expression;

import org.pangrova.parser.ast.Expression;
import org.pangrova.parser.ast.ExpressionType;

/**
 * Represents a subscript (bracket) access expression: {@code base[index]}.
 * Used for array element access ({@code nums[1]}) and map key access ({@code attrs['key']}).
 */
public record SubscriptExpression(Expression base, Expression index) implements Expression {

    @Override
    public ExpressionType type() {
        return ExpressionType.SUBSCRIPT;
    }
}
