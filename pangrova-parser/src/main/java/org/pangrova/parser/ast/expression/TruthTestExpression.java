package org.pangrova.parser.ast.expression;

import org.pangrova.parser.ast.Expression;
import org.pangrova.parser.ast.ExpressionType;

import java.util.Objects;

/** SQL IS [NOT] TRUE/FALSE/UNKNOWN, whose result is never NULL. */
public record TruthTestExpression(Expression operand, TruthValue value, boolean negated) implements Expression {
    public TruthTestExpression {
        Objects.requireNonNull(operand, "operand");
        Objects.requireNonNull(value, "value");
    }

    public enum TruthValue { TRUE, FALSE, UNKNOWN }

    @Override
    public ExpressionType type() {
        return ExpressionType.TRUTH_TEST;
    }
}
