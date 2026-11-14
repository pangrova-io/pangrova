package org.pangrova.parser.api.ast.expression;

import org.pangrova.parser.api.ast.Expression;
import org.pangrova.parser.api.enums.ExpressionType;

/**
 * Represents a NOT expression (e.g., NOT x).
 */
public record NotExpression(Expression expression) implements Expression {

    @Override
    public ExpressionType type() {
        return ExpressionType.NOT;
    }
}
