package org.pangrova.parser.ast.expression;

import org.pangrova.parser.ast.Expression;
import org.pangrova.parser.ast.Query;

/**
 * Represents EXISTS (subquery).
 */
public record ExistsExpression(Query subquery) implements Expression {}
