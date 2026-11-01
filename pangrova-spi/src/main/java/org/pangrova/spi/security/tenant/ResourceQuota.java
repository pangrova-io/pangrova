package org.pangrova.spi.security.tenant;

import org.pangrova.common.api.annotation.Experimental;

/**
 * Hard limits governing a tenant's resource consumption.
 */
@Experimental
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
