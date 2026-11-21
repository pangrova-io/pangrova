package org.pangrova.parser.api.ast.expression;

import org.pangrova.parser.api.ast.Expression;
import org.pangrova.parser.api.enums.ExpressionType;

/**
 * Represents expr IS [NOT] NULL.
 */
public record IsNullExpression(Expression operand, boolean negated) implements Expression {

    @Override
    public ExpressionType type() {
        return ExpressionType.IS_NULL;
    }
}
