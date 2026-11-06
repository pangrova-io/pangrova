package org.pangrova.parser.api.ast.expression;

import org.pangrova.parser.api.ast.Expression;
import org.pangrova.parser.api.ast.ExpressionType;
import org.pangrova.parser.api.ast.Query;

/**
 * Represents a scalar subquery expression, e.g. (SELECT COUNT(*) FROM t).
 */
public record SubqueryExpression(Query subquery) implements Expression {

    @Override
    public ExpressionType type() {
        return ExpressionType.SUBQUERY;
    }
}
