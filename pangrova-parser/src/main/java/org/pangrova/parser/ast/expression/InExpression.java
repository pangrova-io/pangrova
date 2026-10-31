package org.pangrova.parser.ast.expression;

import java.util.List;

import org.pangrova.parser.ast.Expression;
import org.pangrova.parser.ast.ExpressionType;

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
