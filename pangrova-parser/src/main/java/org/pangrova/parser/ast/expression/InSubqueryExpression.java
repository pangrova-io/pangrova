package org.pangrova.parser.ast.expression;

import org.pangrova.parser.ast.Expression;
import org.pangrova.parser.ast.Query;

/**
 * Represents an IN-subquery expression, e.g. x IN (SELECT ...).
 */
public record InSubqueryExpression(Expression value, Query subquery) implements Expression {
}
