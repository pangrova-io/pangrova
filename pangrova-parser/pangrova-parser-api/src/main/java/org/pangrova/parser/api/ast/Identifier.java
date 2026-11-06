package org.pangrova.parser.api.ast;

import java.util.List;

/**
 * Represents a qualified name (e.g., catalog.schema.table or just column).
 */
public record Identifier(List<String> parts) implements Expression {

    public Identifier {
        parts = List.copyOf(parts);
    }

    public static Identifier of(String... parts) {
        return new Identifier(List.of(parts));
    }

    @Override
    public ExpressionType type() {
        return ExpressionType.IDENTIFIER;
    }
}
