package org.pangrova.parser.api.ast.expression;

import java.util.List;

import org.pangrova.parser.api.ast.Expression;
import org.pangrova.parser.api.enums.ExpressionType;

/**
 * Represents an IN-list expression, e.g. x IN (1, 2, 3).
 */
public record InExpression(Expression value, List<Expression> list) implements Expression {

    public InExpression {
        list = List.copyOf(list);
    }

    @Override
    public ExpressionType type() {
        return ExpressionType.IN;
    }
}
