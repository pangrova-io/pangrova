package org.pangrova.parser.ast.expression;

import org.pangrova.parser.ast.Expression;

/**
 * Represents a binary expression (e.g., a + b, x = y, p AND q).
 */
public record BinaryExpression(Expression left, Operator operator, Expression right) implements Expression {

    public enum Operator {
        ADD,
        SUBTRACT,
        MULTIPLY,
        DIVIDE,
        MODULO,
        CONCAT,
        EQUAL,
        NOT_EQUAL,
        LESS_THAN,
        GREATER_THAN,
        LESS_THAN_OR_EQUAL,
        GREATER_THAN_OR_EQUAL,
        AND,
        OR
    }
}
