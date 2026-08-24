package org.pangrova.security.tenant.model;

/**
 * Hard limits governing a tenant's resource consumption.
 */
public record ResourceQuota(
        long maxMemoryBytes,
        int maxConcurrentQueries,
        int maxQueuedQueries,
        long maxCpuTimePerQueryMs,
        long maxScanBytesPerQuery,
        long maxDailyQueryCount,
        long maxDailyScanBytes,
        long maxStorageBytes
) {}
