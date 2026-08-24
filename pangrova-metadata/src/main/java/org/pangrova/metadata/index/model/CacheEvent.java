package org.pangrova.metadata.index.model;

import java.time.Instant;

/**
 * Represents a cache invalidation event for cross-coordinator coordination.
 */
public record CacheEvent(
        long eventId,
        String eventType,
        String indexId,
        String catalogName,
        String schemaName,
        String tableName,
        Instant createdAt
) {}
