package org.pangrova.parser.ast.expression;

import org.pangrova.parser.ast.Expression;
import org.pangrova.parser.ast.ExpressionType;

/**
 * Represents expr IS [NOT] NULL.
 */
public record IsNullExpression(Expression operand, boolean negated) implements Expression {

    @Override
    public ExpressionType type() {
        return ExpressionType.IS_NULL;
    }
}
