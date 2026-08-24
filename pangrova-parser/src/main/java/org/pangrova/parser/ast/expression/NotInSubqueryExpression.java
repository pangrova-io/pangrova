package org.pangrova.parser.ast.expression;

import org.pangrova.parser.ast.Expression;
import org.pangrova.parser.ast.Query;

/**
 * Represents a NOT IN subquery expression, e.g. x NOT IN (SELECT ...).
 */
public record NotInSubqueryExpression(Expression value, Query subquery) implements Expression {
}
