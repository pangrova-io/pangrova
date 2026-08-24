package org.pangrova.security.tenant.sql;

import org.pangrova.security.tenant.model.ResourceQuota;

final class TenantDefaults {

    private static final long MAX_MEMORY_BYTES = 10_737_418_240L;
    private static final int MAX_CONCURRENT_QUERIES = 10;
    private static final int MAX_QUEUED_QUERIES = 100;
    private static final long MAX_CPU_TIME_PER_QUERY_MS = 3_600_000L;
    private static final long MAX_SCAN_BYTES_PER_QUERY = 107_374_182_400L;
    private static final long MAX_DAILY_QUERY_COUNT = 10_000L;
    private static final long MAX_DAILY_SCAN_BYTES = 1_099_511_627_776L;
    private static final long MAX_STORAGE_BYTES = 1_099_511_627_776L;

    private TenantDefaults() {}

    static ResourceQuota resourceQuota() {
        return new ResourceQuota(
                MAX_MEMORY_BYTES,
                MAX_CONCURRENT_QUERIES,
                MAX_QUEUED_QUERIES,
                MAX_CPU_TIME_PER_QUERY_MS,
                MAX_SCAN_BYTES_PER_QUERY,
                MAX_DAILY_QUERY_COUNT,
                MAX_DAILY_SCAN_BYTES,
                MAX_STORAGE_BYTES);
    }
}
