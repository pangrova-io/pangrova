package org.pangrova.spi.index.predicate;

import java.util.List;
import java.util.Objects;

import org.pangrova.spi.annotation.Experimental;

/**
 * Predicate for full-text matching against an inverted index.
 *
 * @param column target column name (must not be blank)
 * @param tokens non-empty list of search tokens (defensively copied)
 * @param mode   matching mode (ALL, ANY, or PHRASE)
 */
@Experimental
public record TextMatch(
        String column,
        List<String> tokens,
        TextMatchMode mode
) implements IndexPredicate {

    public TextMatch {
        if (column == null || column.isBlank()) {
            throw new IllegalArgumentException("column must not be blank");
        }
        Objects.requireNonNull(tokens, "tokens must not be null");
        if (tokens.isEmpty()) {
            throw new IllegalArgumentException("tokens must not be empty");
        }
        tokens = List.copyOf(tokens);
        Objects.requireNonNull(mode, "mode must not be null");
    }
}
