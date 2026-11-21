package org.pangrova.parser.api.ast.expression;

import org.pangrova.parser.api.ast.Expression;
import org.pangrova.parser.api.enums.ExpressionType;
import org.pangrova.parser.api.ast.Query;

/**
 * Represents a NOT IN subquery expression, e.g. x NOT IN (SELECT ...).
 */
public record NotInSubqueryExpression(Expression value, Query subquery) implements Expression {

    @Override
    public ExpressionType type() {
        return ExpressionType.NOT_IN_SUBQUERY;
    }
}
