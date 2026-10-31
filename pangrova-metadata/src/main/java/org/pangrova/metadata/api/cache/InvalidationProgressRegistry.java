package org.pangrova.metadata.api.cache;

/**
 * Publishes this Coordinator's processed DDL invalidation watermark and exposes
 * the cluster-wide floor used to bound invalidation-log cleanup. Implemented by
 * the composition layer; Metadata never reads another owner's coordination store
 * directly.
 */
public interface InvalidationProgressRegistry {

    /** Reports the last processed invalidation log id; failures must not stop local polling. */
    void updateLastProcessedInvalidationId(String coordinatorId, long invalidationId);

    /**
     * Minimum watermark reported by registered coordinators, or {@link Long#MAX_VALUE}
     * when none are registered. An unreported ({@code -1}) watermark lowers the floor.
     */
    long minimumReportedInvalidationId();
}
