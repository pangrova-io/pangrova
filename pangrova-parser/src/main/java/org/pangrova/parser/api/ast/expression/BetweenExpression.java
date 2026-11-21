package org.pangrova.parser.api.ast.expression;

import org.pangrova.parser.api.ast.Expression;
import org.pangrova.parser.api.enums.ExpressionType;

/**
 * Represents expr BETWEEN low AND high.
 */
public record BetweenExpression(Expression value, Expression low, Expression high) implements Expression {

    @Override
    public ExpressionType type() {
        return ExpressionType.BETWEEN;
    }
}
