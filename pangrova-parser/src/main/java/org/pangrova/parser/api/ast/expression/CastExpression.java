package org.pangrova.parser.api.ast.expression;

import org.pangrova.parser.api.ast.Expression;
import org.pangrova.parser.api.enums.ExpressionType;

/**
 * CAST(expression AS dataType) expression node.
 */
public record CastExpression(Expression operand, String targetType) implements Expression {

    @Override
    public ExpressionType type() {
        return ExpressionType.CAST;
    }
}
