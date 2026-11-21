package org.pangrova.security.api.tenant;

import org.pangrova.security.api.enums.SchedulingPolicy;

import org.pangrova.common.api.annotation.Experimental;

/**
 * Isolated resource pool with scheduling, capacity, and elastic borrowing configuration.
 */
@Experimental
public record ResourcePool(
        String poolId,
        String name,
        double cpuShare,
        long memoryBytes,
        int maxConcurrentQueries,
        int queueCapacity,
        SchedulingPolicy policy,
        int priority,
        boolean preemptible,
        double minShare,
        double maxShare,
        boolean elasticEnabled
) {}
