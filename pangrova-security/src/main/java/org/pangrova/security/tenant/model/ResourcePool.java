package org.pangrova.security.tenant.model;

/**
 * Isolated resource pool with scheduling, capacity, and elastic borrowing configuration.
 */
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
