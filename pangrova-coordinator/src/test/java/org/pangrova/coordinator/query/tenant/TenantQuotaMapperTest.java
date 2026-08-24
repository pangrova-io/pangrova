package org.pangrova.coordinator.query.tenant;

import org.pangrova.security.tenant.model.ResourceQuota;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Validates the unified quota model bridge (F-CHAIN-RES-03).
 *
 * <p>After removal of the parallel TenantResourceQuota, ResourceQuota is the
 * sole authority. TenantQuotaMapper documents and validates the mapping.</p>
 */
class TenantQuotaMapperTest {

    private static final ResourceQuota FULL_QUOTA = new ResourceQuota(
            10737418240L, 10, 100, 3600000L,
            107374182400L, 10000L, 1099511627776L, 1099511627776L);

    private static final ResourceQuota UNLIMITED_MEMORY = new ResourceQuota(
            0, 10, 100, 3600000L,
            107374182400L, 10000L, 1099511627776L, 1099511627776L);

    @Test
    void hasMemoryLimit_trueForPositiveLimit() {
        assertThat(TenantQuotaMapper.hasMemoryLimit(FULL_QUOTA)).isTrue();
    }

    @Test
    void hasMemoryLimit_falseForZeroLimit() {
        assertThat(TenantQuotaMapper.hasMemoryLimit(UNLIMITED_MEMORY)).isFalse();
    }

    @Test
    void hasMemoryLimit_falseForNull() {
        assertThat(TenantQuotaMapper.hasMemoryLimit(null)).isFalse();
    }

    @Test
    void hasConcurrencyLimit_trueForPositiveLimit() {
        assertThat(TenantQuotaMapper.hasConcurrencyLimit(FULL_QUOTA)).isTrue();
    }

    @Test
    void effectiveMemoryBytes_returnsPositiveForCapped() {
        assertThat(TenantQuotaMapper.effectiveMemoryBytes(FULL_QUOTA))
                .isEqualTo(10737418240L);
    }

    @Test
    void effectiveMemoryBytes_zeroForUnlimited() {
        assertThat(TenantQuotaMapper.effectiveMemoryBytes(UNLIMITED_MEMORY)).isZero();
    }

    @Test
    void effectiveMemoryBytes_zeroForNull() {
        assertThat(TenantQuotaMapper.effectiveMemoryBytes(null)).isZero();
    }

    @Test
    void effectiveConcurrentQueries_returnsLimit() {
        assertThat(TenantQuotaMapper.effectiveConcurrentQueries(FULL_QUOTA))
                .isEqualTo(10);
    }

    @Test
    void effectiveConcurrentQueries_maxValueForNull() {
        assertThat(TenantQuotaMapper.effectiveConcurrentQueries(null))
                .isEqualTo(Integer.MAX_VALUE);
    }

    @Test
    void validate_nullReturnsError() {
        assertThat(TenantQuotaMapper.validate(null)).isNotNull();
    }

    @Test
    void validate_validQuotaReturnsNull() {
        assertThat(TenantQuotaMapper.validate(FULL_QUOTA)).isNull();
    }
}
