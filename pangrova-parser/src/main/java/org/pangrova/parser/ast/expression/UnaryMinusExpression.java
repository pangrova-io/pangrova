package org.pangrova.parser.ast.expression;

import org.pangrova.parser.ast.Expression;

/**
 * Represents a unary minus expression (e.g., -1, -amount).
 */
public record UnaryMinusExpression(Expression operand) implements Expression {}
