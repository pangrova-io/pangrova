package org.pangrova.parser.ast.expression;

import org.pangrova.parser.ast.Expression;

/**
 * CAST(expression AS dataType) expression node.
 */
public record CastExpression(Expression operand, String targetType) implements Expression {
}
