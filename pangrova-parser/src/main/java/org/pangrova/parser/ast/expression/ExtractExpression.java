package org.pangrova.parser.ast.expression;

import org.pangrova.parser.ast.Expression;

import java.util.Objects;

/**
 * Represents EXTRACT(field FROM expression), e.g. EXTRACT(YEAR FROM o_orderdate).
 */
public record ExtractExpression(String field, Expression source) implements Expression {
    public ExtractExpression {
        Objects.requireNonNull(field, "extract field must not be null");
        Objects.requireNonNull(source, "extract source must not be null");
    }
}
