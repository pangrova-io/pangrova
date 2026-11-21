package org.pangrova.parser.api.ast;

import org.pangrova.parser.api.enums.QueryHintType;

import java.util.Locale;
import java.util.List;
import java.util.Objects;

/**
 * Represents a query hint extracted from comment syntax: {@code /*+ HINT_NAME(params) * /}.
 * Supports hints for join strategy, index usage, parallelism, and timeout.
 */
public record QueryHint(QueryHintType hintType, String name, List<String> parameters) {

    public QueryHint {
        Objects.requireNonNull(hintType, "hint type must not be null");
        Objects.requireNonNull(name, "hint name must not be null");
        parameters = List.copyOf(parameters);
    }

    /** Parses a hint name into its typed category. */
    public static QueryHintType resolveType(String hintName) {
        return switch (hintName.toUpperCase(Locale.ROOT)) {
            case "BROADCAST", "SHUFFLE", "SORT_MERGE" -> QueryHintType.JOIN_STRATEGY;
            case "USE_INDEX", "NO_INDEX" -> QueryHintType.INDEX_USAGE;
            case "PARALLEL" -> QueryHintType.PARALLELISM;
            case "QUERY_TIMEOUT" -> QueryHintType.TIMEOUT;
            default -> QueryHintType.GENERIC;
        };
    }

    /** Creates a hint from name and parameters, auto-resolving the type. */
    public static QueryHint of(String name, List<String> parameters) {
        return new QueryHint(resolveType(name), name.toUpperCase(Locale.ROOT), parameters);
    }
}
