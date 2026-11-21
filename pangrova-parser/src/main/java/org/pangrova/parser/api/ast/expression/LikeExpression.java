package org.pangrova.parser.api.ast.expression;

import org.pangrova.parser.api.ast.Expression;
import org.pangrova.parser.api.enums.ExpressionType;

/**
 * Represents expr LIKE pattern.
 */
public record LikeExpression(Expression value, Expression pattern) implements Expression {

    @Override
    public ExpressionType type() {
        return ExpressionType.LIKE;
    }
}
