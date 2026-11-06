package org.pangrova.parser.api.ast.expression;

import org.pangrova.parser.api.ast.Expression;
import org.pangrova.parser.api.ast.ExpressionType;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * Represents a STRUCT/ROW constructor expression.
 *
 * <p>Supports both positional syntax (STRUCT(expr1, expr2)) and named syntax
 * (ROW(a => 1, b => 'x')). Fields may be a mix of named and positional.</p>
 */
public record StructConstructor(List<Field> fields) implements Expression {

    public StructConstructor {
        Objects.requireNonNull(fields, "struct fields must not be null");
        if (fields.isEmpty()) {
            throw new IllegalArgumentException("STRUCT/ROW constructor requires at least one field");
        }
        fields = List.copyOf(fields);
    }

    /**
     * A single field in a STRUCT/ROW constructor, optionally named.
     *
     * @param name  field name if using named syntax (a => expr), empty for positional
     * @param value the field value expression
     */
    public record Field(Optional<String> name, Expression value) {

        public Field {
            Objects.requireNonNull(name, "field name optional must not be null");
            Objects.requireNonNull(value, "field value must not be null");
        }

        /**
         * Creates a positional (unnamed) field.
         */
        public static Field positional(Expression value) {
            return new Field(Optional.empty(), value);
        }

        /**
         * Creates a named field (e.g., a => 1).
         */
        public static Field named(String name, Expression value) {
            Objects.requireNonNull(name, "field name must not be null");
            return new Field(Optional.of(name), value);
        }
    }

    @Override
    public ExpressionType type() {
        return ExpressionType.STRUCT_CONSTRUCTOR;
    }
}
