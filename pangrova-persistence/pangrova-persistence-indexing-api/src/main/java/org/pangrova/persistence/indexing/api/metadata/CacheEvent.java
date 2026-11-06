package org.pangrova.persistence.indexing.api.metadata;

import org.pangrova.common.api.annotation.Experimental;

import java.time.Instant;

/**
 * Represents a cache invalidation event for cross-coordinator coordination.
 */
@Experimental
public record CacheEvent(
        long eventId,
        String eventType,
        String indexId,
        String catalogName,
        String schemaName,
        String tableName,
        Instant createdAt
) {}
