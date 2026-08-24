package org.pangrova.coordinator.query.tenant;

import org.pangrova.security.tenant.model.ResourceQuota;

/**
 * Single-boundary bridge between security's {@link ResourceQuota} and coordinator
 * runtime enforcement.
 *
 * <p>After the removal of the parallel {@code TenantResourceQuota} in the resource
 * module (F-RES-07 / F-CHAIN-RES-03), {@link ResourceQuota} is the <b>sole
 * authority</b> for tenant limits. This mapper serves as the documented boundary
 * point where security-defined quotas are validated and translated into
 * coordinator-enforceable dimensions.</p>
 *
 * <h3>Enforcement model</h3>
 * <ul>
 *   <li><b>Coordinator admission gate</b> — {@code QuotaChecker.checkBeforeSubmit}
 *       enforces concurrent queries, queued queries, daily counts, daily scan bytes,
 *       and memory caps. No over-quota query is dispatched to a worker.</li>
 *   <li><b>Coordinator runtime check</b> — {@code QuotaChecker.checkDuringExecution}
 *       enforces per-query CPU time and scan byte limits.</li>
 *   <li><b>Worker-side</b> — Workers enforce process-global memory and thread budgets
 *       only. Per-tenant tracking was intentionally removed because the admission
 *       gate prevents over-quota queries from reaching workers.</li>
 * </ul>
 *
 * <h3>Dimension mapping</h3>
 * <table>
 *   <tr><th>ResourceQuota field</th><th>Enforcement point</th></tr>
 *   <tr><td>maxMemoryBytes</td><td>Pre-submit admission (coordinator)</td></tr>
 *   <tr><td>maxConcurrentQueries</td><td>Pre-submit admission (coordinator)</td></tr>
 *   <tr><td>maxQueuedQueries</td><td>Pre-submit admission (coordinator)</td></tr>
 *   <tr><td>maxCpuTimePerQueryMs</td><td>Runtime check (coordinator)</td></tr>
 *   <tr><td>maxScanBytesPerQuery</td><td>Runtime check (coordinator)</td></tr>
 *   <tr><td>maxDailyQueryCount</td><td>Pre-submit admission (coordinator)</td></tr>
 *   <tr><td>maxDailyScanBytes</td><td>Pre-submit admission (coordinator)</td></tr>
 *   <tr><td>maxStorageBytes</td><td>DDL-time enforcement (future)</td></tr>
 * </table>
 */
public final class TenantQuotaMapper {

    private TenantQuotaMapper() {}

    /**
     * Returns {@code true} if the quota imposes a memory limit that should be
     * enforced at admission. A value of 0 (or negative) means "unlimited".
     */
    public static boolean hasMemoryLimit(ResourceQuota quota) {
        return quota != null && quota.maxMemoryBytes() > 0;
    }

    /**
     * Returns {@code true} if the quota imposes a concurrency limit.
     */
    public static boolean hasConcurrencyLimit(ResourceQuota quota) {
        return quota != null && quota.maxConcurrentQueries() > 0;
    }

    /**
     * Returns the effective memory limit in bytes, or 0 for unlimited.
     *
     * @param quota the tenant's resource quota (may be null)
     * @return effective memory limit; 0 means unlimited
     */
    public static long effectiveMemoryBytes(ResourceQuota quota) {
        return (quota != null && quota.maxMemoryBytes() > 0) ? quota.maxMemoryBytes() : 0;
    }

    /**
     * Returns the effective max concurrent queries, or {@link Integer#MAX_VALUE}
     * if no limit is set.
     *
     * @param quota the tenant's resource quota (may be null)
     * @return effective concurrency limit
     */
    public static int effectiveConcurrentQueries(ResourceQuota quota) {
        return (quota != null && quota.maxConcurrentQueries() > 0)
                ? quota.maxConcurrentQueries() : Integer.MAX_VALUE;
    }

    /**
     * Validates that a quota has all required fields for runtime enforcement.
     * Returns a human-readable validation error, or null if valid.
     *
     * @param quota the quota to validate
     * @return error message, or null if valid
     * @throws NullPointerException if quota is null
     */
    public static String validate(ResourceQuota quota) {
        if (quota == null) {
            return "quota must not be null";
        }
        if (quota.maxConcurrentQueries() < 0) {
            return "maxConcurrentQueries must not be negative";
        }
        if (quota.maxMemoryBytes() < 0) {
            return "maxMemoryBytes must not be negative";
        }
        return null;
    }
}
