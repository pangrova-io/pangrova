package org.pangrova.parser.api.enums;

/** Supported hint categories. */
public enum QueryHintType {
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
