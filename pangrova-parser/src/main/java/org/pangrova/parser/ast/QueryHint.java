package org.pangrova.parser.ast;

import java.util.List;
import java.util.Objects;

/**
 * Represents a query hint extracted from comment syntax: {@code /*+ HINT_NAME(params) * /}.
 * Supports hints for join strategy, index usage, parallelism, and timeout.
 */
public record QueryHint(HintType hintType, String name, List<String> parameters) {

    public QueryHint {
        Objects.requireNonNull(hintType, "hint type must not be null");
        Objects.requireNonNull(name, "hint name must not be null");
        parameters = List.copyOf(parameters);
    }

    /** Supported hint categories. */
    public enum HintType {
        /** Join strategy hints: BROADCAST, SHUFFLE, SORT_MERGE. */
        JOIN_STRATEGY,
        /** Index usage hints: USE_INDEX, NO_INDEX. */
        INDEX_USAGE,
        /** Parallelism hint: PARALLEL(n). */
        PARALLELISM,
        /** Timeout hint: QUERY_TIMEOUT(ms). */
        TIMEOUT,
        /** Generic unrecognized hint (preserved for forward compatibility). */
        GENERIC
    }

    /** Parses a hint name into its typed category. */
    public static HintType resolveType(String hintName) {
        return switch (hintName.toUpperCase()) {
            case "BROADCAST", "SHUFFLE", "SORT_MERGE" -> HintType.JOIN_STRATEGY;
            case "USE_INDEX", "NO_INDEX" -> HintType.INDEX_USAGE;
            case "PARALLEL" -> HintType.PARALLELISM;
            case "QUERY_TIMEOUT" -> HintType.TIMEOUT;
            default -> HintType.GENERIC;
        };
    }

    /** Creates a hint from name and parameters, auto-resolving the type. */
    public static QueryHint of(String name, List<String> parameters) {
        return new QueryHint(resolveType(name), name.toUpperCase(), parameters);
    }
}
