package org.pangrova.security.core.tenant;

import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.constraints.IntRange;
import net.jqwik.api.constraints.LongRange;
import org.pangrova.security.api.tenant.ResourceQuota;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Property tests for {@link QuotaChecker} boundary enforcement.
 */
class QuotaCheckerPropertyTest {

    private final QuotaChecker checker = new QuotaChecker();

    @Property(tries = 10)
    void belowAllLimits_alwaysPasses(
            @ForAll @IntRange(min = 0, max = 4) int concurrent,
            @ForAll @IntRange(min = 0, max = 4) int queued) {

        ResourceQuota quota = new ResourceQuota(
                10737418240L, 5, 100, 3600000L,
                107374182400L, 10000L, 1099511627776L, 1099511627776L);
        TenantUsage usage = new TenantUsage(concurrent, queued, 0L, 0L);

        QuotaCheckResult result = checker.checkBeforeSubmit(quota, usage);
        assertThat(result.passed()).isTrue();
    }

    @Property(tries = 10)
    void atOrAboveConcurrentLimit_fails(
            @ForAll @IntRange(min = 5, max = 100) int concurrent) {

        ResourceQuota quota = new ResourceQuota(
                10737418240L, 5, 100, 3600000L,
                107374182400L, 10000L, 1099511627776L, 1099511627776L);
        TenantUsage usage = new TenantUsage(concurrent, 0, 0L, 0L);

        QuotaCheckResult result = checker.checkBeforeSubmit(quota, usage);
        assertThat(result.passed()).isFalse();
    }

    @Property(tries = 10)
    void cpuTimeExceeded_throwsException(
            @ForAll @LongRange(min = 3600001, max = 10000000) long cpuTimeMs) {

        ResourceQuota quota = new ResourceQuota(
                10737418240L, 5, 100, 3600000L,
                107374182400L, 10000L, 1099511627776L, 1099511627776L);

        assertThatThrownBy(() -> checker.checkDuringExecution(quota, cpuTimeMs, 0))
                .isInstanceOf(TenantQuotaExceededException.class);
    }
}
