package org.pangrova.metadata.cache;

import java.time.Instant;
import java.util.Objects;

/**
 * Immutable record representing a single cache invalidation event persisted in the
 * {@code invalidation_log} table. Each entry identifies a cache key and the type
 * of invalidation (table-level or catalog-level).
 *
 * <p>The {@code updatedAt} field serves as the high watermark for polling. Using
 * a timestamp instead of the PK {@code id} prevents out-of-order misses when
 * rows are committed in non-monotonic ID order.</p>
 */
public record InvalidationEntry(
        long id,
        String cacheKey,
        InvalidationType invalidationType,
        Instant createdAt,
        Instant updatedAt
) {
    public InvalidationEntry {
        Objects.requireNonNull(cacheKey, "cacheKey must not be null");
        Objects.requireNonNull(invalidationType, "invalidationType must not be null");
        Objects.requireNonNull(createdAt, "createdAt must not be null");
        Objects.requireNonNull(updatedAt, "updatedAt must not be null");
    }

    /**
     * Type of cache invalidation.
     */
    public enum InvalidationType {
        TABLE,
        CATALOG,
        /** Invalidates query result cache entries referencing a mutated table. */
        RESULT_CACHE
    }
}
