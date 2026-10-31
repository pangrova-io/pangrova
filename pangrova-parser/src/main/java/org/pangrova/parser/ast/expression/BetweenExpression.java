package org.pangrova.parser.ast.expression;

import org.pangrova.parser.ast.Expression;
import org.pangrova.parser.ast.ExpressionType;

/**
 * Represents expr BETWEEN low AND high.
 */
public record BetweenExpression(Expression value, Expression low, Expression high) implements Expression {

    @Override
    public ExpressionType type() {
        return ExpressionType.BETWEEN;
    }
}
