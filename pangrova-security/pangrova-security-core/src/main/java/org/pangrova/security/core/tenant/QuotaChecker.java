package org.pangrova.security.core.tenant;

import org.pangrova.security.api.tenant.ResourceQuota;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * Enforces tenant resource quotas at pre-submit and runtime stages.
 */
public class QuotaChecker {

    /**
     * Pre-submit check: validates concurrent, queued, daily query count, and daily scan bytes.
     *
     * @param quota the tenant's resource quota
     * @param usage the tenant's current usage
     * @return the quota check result
     * @throws NullPointerException if quota or usage is null
     */
    public QuotaCheckResult checkBeforeSubmit(ResourceQuota quota, TenantUsage usage) {
        Objects.requireNonNull(quota, "quota must not be null");
        Objects.requireNonNull(usage, "usage must not be null");
        List<String> violations = new ArrayList<>();
        checkLimit(violations, usage.concurrentQueries(), quota.maxConcurrentQueries(), "concurrent queries");
        checkLimit(violations, usage.queuedQueries(), quota.maxQueuedQueries(), "queued queries");
        checkLimit(violations, usage.dailyQueryCount(), quota.maxDailyQueryCount(), "daily query count");
        checkLimit(violations, usage.dailyScanBytes(), quota.maxDailyScanBytes(), "daily scan bytes");
        return violations.isEmpty() ? QuotaCheckResult.pass() : QuotaCheckResult.fail(violations);
    }

    /**
     * Runtime check: throws on CPU time or scan bytes violation.
     */
    public void checkDuringExecution(ResourceQuota quota, long cpuTimeMs, long inputBytes) {
        if (cpuTimeMs > quota.maxCpuTimePerQueryMs()) {
            throw new TenantQuotaExceededException(
                    "CPU time exceeded: " + cpuTimeMs + "ms > " + quota.maxCpuTimePerQueryMs() + "ms");
        }
        if (inputBytes > quota.maxScanBytesPerQuery()) {
            throw new TenantQuotaExceededException(
                    "Scan bytes exceeded: " + inputBytes + " > " + quota.maxScanBytesPerQuery());
        }
    }

    private void checkLimit(List<String> violations, long current, long max, String metric) {
        if (current >= max) {
            violations.add(metric + ": " + current + " >= " + max);
        }
    }
}
