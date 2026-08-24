package org.pangrova.parser.ast.expression;

import org.pangrova.parser.ast.Expression;

/**
 * Represents expr LIKE pattern.
 */
public record LikeExpression(Expression value, Expression pattern) implements Expression {}
