package org.pangrova.security.core.tenant;

/**
 * Snapshot of a tenant's current resource consumption for quota evaluation.
 */
public record TenantUsage(
        int concurrentQueries,
        int queuedQueries,
        long dailyQueryCount,
        long dailyScanBytes
) {}
