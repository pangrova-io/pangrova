package org.pangrova.parser.ast.expression;

import org.pangrova.parser.ast.Expression;
import org.pangrova.parser.ast.ExpressionType;

/**
 * Represents a unary minus expression (e.g., -1, -amount).
 */
public record UnaryMinusExpression(Expression operand) implements Expression {

    @Override
    public ExpressionType type() {
        return ExpressionType.UNARY_MINUS;
    }
}
