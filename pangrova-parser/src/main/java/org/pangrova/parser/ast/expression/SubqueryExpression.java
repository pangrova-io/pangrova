package org.pangrova.parser.ast.expression;

import org.pangrova.parser.ast.Expression;
import org.pangrova.parser.ast.Query;

/**
 * Represents a scalar subquery expression, e.g. (SELECT COUNT(*) FROM t).
 */
public record SubqueryExpression(Query subquery) implements Expression {
}
