package org.pangrova.parser.ast.expression;

import org.pangrova.parser.ast.Expression;
import org.pangrova.parser.ast.ExpressionType;

import java.util.List;
import java.util.Objects;

/**
 * Represents an ARRAY constructor expression, e.g. ARRAY[1.0, 2.0, 3.0].
 */
public record ArrayConstructor(List<Expression> elements) implements Expression {

    public ArrayConstructor {
        Objects.requireNonNull(elements, "array elements must not be null");
        elements = List.copyOf(elements);
    }

    @Override
    public ExpressionType type() {
        return ExpressionType.ARRAY_CONSTRUCTOR;
    }
}
