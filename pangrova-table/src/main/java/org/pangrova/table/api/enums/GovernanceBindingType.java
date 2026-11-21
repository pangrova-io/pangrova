package org.pangrova.table.api.enums;

import java.util.Locale;
import java.util.Optional;

/** Governance artifacts that can be bound to a controlled table. */
public enum GovernanceBindingType {
    COLUMN_TAG,
    COLUMN_MASK,
    ROW_FILTER,
    QUALITY_RULE,
    LINEAGE;

    /** Parses a case-insensitive binding type token; unknown tokens return empty. */
    public static Optional<GovernanceBindingType> parse(String token) {
        if (token == null || token.isBlank()) {
            return Optional.empty();
        }
        try {
            return Optional.of(valueOf(token.trim().toUpperCase(Locale.ROOT)));
        } catch (IllegalArgumentException e) {
            return Optional.empty();
        }
    }
}
