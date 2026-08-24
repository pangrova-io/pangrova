package org.pangrova.parser.ast.expression;

import org.pangrova.parser.ast.Expression;

/**
 * Represents a NOT expression (e.g., NOT x).
 */
public record NotExpression(Expression expression) implements Expression {
}
