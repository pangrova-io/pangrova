package org.pangrova.parser.ast.expression;

import java.util.List;

import org.pangrova.parser.ast.Expression;

/**
 * Represents a NOT IN list expression, e.g. x NOT IN (1, 2, 3).
 */
public record NotInExpression(Expression value, List<Expression> list) implements Expression {

    public NotInExpression {
        list = List.copyOf(list);
    }
}
