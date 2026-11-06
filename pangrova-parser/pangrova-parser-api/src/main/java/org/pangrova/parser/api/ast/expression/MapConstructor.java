package org.pangrova.parser.api.ast.expression;

import org.pangrova.parser.api.ast.Expression;
import org.pangrova.parser.api.ast.ExpressionType;

import java.util.List;
import java.util.Objects;

/**
 * Represents a MAP constructor expression, e.g. MAP['key1', val1, 'key2', val2].
 *
 * <p>Entries are stored as a flat list of alternating key-value expressions.
 * The list length must be even (each key has a corresponding value).</p>
 */
public record MapConstructor(List<Expression> entries) implements Expression {

    public MapConstructor {
        Objects.requireNonNull(entries, "map entries must not be null");
        if (entries.size() % 2 != 0) {
            throw new IllegalArgumentException(
                    "MAP constructor requires an even number of expressions (key-value pairs)");
        }
        entries = List.copyOf(entries);
    }

    /**
     * Returns the number of key-value pairs in this map literal.
     */
    public int size() {
        return entries.size() / 2;
    }

    /**
     * Returns the key expression at the given pair index.
     */
    public Expression key(int index) {
        return entries.get(index * 2);
    }

    /**
     * Returns the value expression at the given pair index.
     */
    public Expression value(int index) {
        return entries.get(index * 2 + 1);
    }

    @Override
    public ExpressionType type() {
        return ExpressionType.MAP_CONSTRUCTOR;
    }
}
