package org.pangrova.parser.api.ast;

import java.util.Objects;
import java.util.Optional;

/**
 * Represents a derived table (subquery in FROM clause) with a required alias.
 * Example: (SELECT ... FROM ...) AS alias
 */
public record DerivedTableRef(Statement subquery, Optional<String> alias) {
    public DerivedTableRef {
        Objects.requireNonNull(subquery, "derived table subquery must not be null");
        Objects.requireNonNull(alias, "alias must not be null");
    }
}
