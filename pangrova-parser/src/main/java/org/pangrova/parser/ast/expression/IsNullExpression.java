package org.pangrova.parser.ast.expression;

import org.pangrova.parser.ast.Expression;

/**
 * Represents expr IS [NOT] NULL.
 */
public record IsNullExpression(Expression operand, boolean negated) implements Expression {}
