package org.pangrova.coordinator.query.resourcegroup;

import java.util.Objects;

/**
 * Configuration for a single resource group: concurrency limit, queue depth,
 * and memory limit.
 *
 * @param groupId           unique resource group identifier
 * @param maxConcurrency    maximum concurrent queries in this group
 * @param maxQueueDepth     maximum queued queries before rejection
 * @param maxMemoryBytes    maximum aggregate estimated memory for admitted queries (0 = unlimited)
 */
public record ResourceGroupConfig(
        String groupId,
        int maxConcurrency,
        int maxQueueDepth,
        long maxMemoryBytes) {

    public ResourceGroupConfig {
        Objects.requireNonNull(groupId, "groupId must not be null");
        if (groupId.isBlank()) {
            throw new IllegalArgumentException("groupId must not be blank");
        }
        if (maxConcurrency < 1) {
            throw new IllegalArgumentException("maxConcurrency must be >= 1, got " + maxConcurrency);
        }
        if (maxQueueDepth < 0) {
            throw new IllegalArgumentException("maxQueueDepth must be >= 0, got " + maxQueueDepth);
        }
        if (maxMemoryBytes < 0) {
            throw new IllegalArgumentException("maxMemoryBytes must be >= 0, got " + maxMemoryBytes);
        }
    }

    /** Creates a config with unlimited memory. */
    public static ResourceGroupConfig of(String groupId, int maxConcurrency, int maxQueueDepth) {
        return new ResourceGroupConfig(groupId, maxConcurrency, maxQueueDepth, 0);
    }
}
