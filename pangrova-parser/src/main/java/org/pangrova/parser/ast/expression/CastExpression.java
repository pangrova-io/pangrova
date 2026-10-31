package org.pangrova.parser.ast.expression;

import org.pangrova.parser.ast.Expression;
import org.pangrova.parser.ast.ExpressionType;

/**
 * CAST(expression AS dataType) expression node.
 */
public record CastExpression(Expression operand, String targetType) implements Expression {

    @Override
    public ExpressionType type() {
        return ExpressionType.CAST;
    }
}
