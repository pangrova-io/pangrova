package org.pangrova.parser.api.ast.expression;

import org.pangrova.parser.api.ast.Expression;
import org.pangrova.parser.api.enums.ExpressionType;
import org.pangrova.parser.api.ast.Query;

/**
 * Represents an IN-subquery expression, e.g. x IN (SELECT ...).
 */
public record InSubqueryExpression(Expression value, Query subquery) implements Expression {

    @Override
    public ExpressionType type() {
        return ExpressionType.IN_SUBQUERY;
    }
}
