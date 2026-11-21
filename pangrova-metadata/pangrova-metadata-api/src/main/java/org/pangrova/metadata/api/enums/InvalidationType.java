package org.pangrova.metadata.api.enums;

/** Cache invalidation target. */
public enum InvalidationType {
    TABLE,
    CATALOG,
    /** Invalidates query result cache entries referencing a mutated table. */
    RESULT_CACHE
}
