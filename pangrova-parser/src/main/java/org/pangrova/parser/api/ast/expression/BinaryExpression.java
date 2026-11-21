package org.pangrova.parser.api.ast.expression;

import org.pangrova.parser.api.enums.BinaryExpressionOperator;
import org.pangrova.parser.api.ast.Expression;
import org.pangrova.parser.api.enums.ExpressionType;

/**
 * Represents a binary expression (e.g., a + b, x = y, p AND q).
 */
public record BinaryExpression(Expression left, BinaryExpressionOperator operator,
        Expression right) implements Expression {

    @Override
    public ExpressionType type() {
        return ExpressionType.BINARY;
    }
}
