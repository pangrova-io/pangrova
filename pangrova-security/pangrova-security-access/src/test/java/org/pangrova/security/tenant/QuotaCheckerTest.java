package org.pangrova.security.tenant;

import org.pangrova.security.exception.TenantQuotaExceededException;

import org.pangrova.security.api.tenant.ResourceQuota;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Validates: Requirements 18.6 — QuotaChecker enforces tenant resource quotas.
 */
class QuotaCheckerTest {

    private static final ResourceQuota QUOTA = new ResourceQuota(
            10737418240L, 10, 100, 3600000L,
            107374182400L, 10000L, 1099511627776L, 1099511627776L);

    private final QuotaChecker checker = new QuotaChecker();

    @Test
    void passesWhenUsageBelowLimits() {
        TenantUsage usage = new TenantUsage(5, 10, 100, 1000);
        QuotaCheckResult result = checker.checkBeforeSubmit(QUOTA, usage);
        assertThat(result.passed()).isTrue();
        assertThat(result.violations()).isEmpty();
    }

    @Test
    void failsWhenConcurrentQueriesExceeded() {
        TenantUsage usage = new TenantUsage(10, 0, 0, 0);
        QuotaCheckResult result = checker.checkBeforeSubmit(QUOTA, usage);
        assertThat(result.passed()).isFalse();
        assertThat(result.violations()).anyMatch(v -> v.contains("concurrent queries"));
    }

    @Test
    void failsWhenQueuedQueriesExceeded() {
        TenantUsage usage = new TenantUsage(0, 100, 0, 0);
        QuotaCheckResult result = checker.checkBeforeSubmit(QUOTA, usage);
        assertThat(result.passed()).isFalse();
        assertThat(result.violations()).anyMatch(v -> v.contains("queued queries"));
    }

    @Test
    void failsWhenDailyQueryCountExceeded() {
        TenantUsage usage = new TenantUsage(0, 0, 10000, 0);
        QuotaCheckResult result = checker.checkBeforeSubmit(QUOTA, usage);
        assertThat(result.passed()).isFalse();
        assertThat(result.violations()).anyMatch(v -> v.contains("daily query count"));
    }

    @Test
    void reportsMultipleViolations() {
        TenantUsage usage = new TenantUsage(10, 100, 10000, 1099511627776L);
        QuotaCheckResult result = checker.checkBeforeSubmit(QUOTA, usage);
        assertThat(result.passed()).isFalse();
        assertThat(result.violations()).hasSizeGreaterThanOrEqualTo(3);
    }

    @Test
    void runtimeCheckThrowsOnCpuTimeExceeded() {
        assertThatThrownBy(() -> checker.checkDuringExecution(QUOTA, 3600001L, 0))
                .isInstanceOf(TenantQuotaExceededException.class)
                .hasMessageContaining("CPU time exceeded");
    }

    @Test
    void runtimeCheckThrowsOnScanBytesExceeded() {
        assertThatThrownBy(() -> checker.checkDuringExecution(QUOTA, 0, 107374182401L))
                .isInstanceOf(TenantQuotaExceededException.class)
                .hasMessageContaining("Scan bytes exceeded");
    }

    @Test
    void runtimeCheckPassesWithinLimits() {
        // Should not throw
        checker.checkDuringExecution(QUOTA, 1000, 1000);
    }

    @Test
    void rejectsNullQuota() {
        TenantUsage usage = new TenantUsage(0, 0, 0, 0);
        assertThatThrownBy(() -> checker.checkBeforeSubmit(null, usage))
                .isInstanceOf(NullPointerException.class);
    }

    @Test
    void rejectsNullUsage() {
        assertThatThrownBy(() -> checker.checkBeforeSubmit(QUOTA, null))
                .isInstanceOf(NullPointerException.class);
    }
}
