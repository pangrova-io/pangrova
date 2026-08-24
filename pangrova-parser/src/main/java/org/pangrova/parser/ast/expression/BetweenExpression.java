package org.pangrova.parser.ast.expression;

import org.pangrova.parser.ast.Expression;

/**
 * Represents expr BETWEEN low AND high.
 */
public record BetweenExpression(Expression value, Expression low, Expression high) implements Expression {}
