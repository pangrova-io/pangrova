package org.pangrova.parser.ast.expression;

import org.pangrova.parser.ast.Expression;
import org.pangrova.parser.ast.ExpressionType;

/**
 * Represents expr LIKE pattern.
 */
public record LikeExpression(Expression value, Expression pattern) implements Expression {

    @Override
    public ExpressionType type() {
        return ExpressionType.LIKE;
    }
}
