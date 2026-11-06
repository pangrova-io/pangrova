package org.pangrova.parser.api.ast.expression;

import org.pangrova.parser.api.ast.Expression;
import org.pangrova.parser.api.ast.ExpressionType;
import org.pangrova.parser.api.ast.Query;

/**
 * Represents EXISTS (subquery).
 */
public record ExistsExpression(Query subquery) implements Expression {

    @Override
    public ExpressionType type() {
        return ExpressionType.EXISTS;
    }
}
