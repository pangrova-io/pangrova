package org.pangrova.parser.ast.expression;

import org.pangrova.parser.ast.Expression;
import org.pangrova.parser.ast.ExpressionType;

/**
 * Represents a NOT expression (e.g., NOT x).
 */
public record NotExpression(Expression expression) implements Expression {

    @Override
    public ExpressionType type() {
        return ExpressionType.NOT;
    }
}
