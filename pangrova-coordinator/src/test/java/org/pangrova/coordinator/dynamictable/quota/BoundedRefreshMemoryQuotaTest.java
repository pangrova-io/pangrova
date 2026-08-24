package org.pangrova.coordinator.dynamictable.quota;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Unit tests for {@link BoundedRefreshMemoryQuota} allocate/release semantics.
 *
 * <p>Relocated from {@code DynamicTableMemoryQuotaE2ETest.memoryQuotaExceededFailsRefresh}
 * — this logic tests the quota class directly without any SQL pipeline or container.</p>
 */
class BoundedRefreshMemoryQuotaTest {

    @Test
    void allocateWithinQuota_succeeds() {
        BoundedRefreshMemoryQuota quota = new BoundedRefreshMemoryQuota(1024);

        quota.allocate(512);

        assertThat(quota.usedBytes()).isEqualTo(512);
    }

    @Test
    void allocateBeyondQuota_throwsWithRequestedAndAvailable() {
        BoundedRefreshMemoryQuota quota = new BoundedRefreshMemoryQuota(1024);
        quota.allocate(512);

        assertThatThrownBy(() -> quota.allocate(1024))
                .isInstanceOf(RefreshMemoryQuotaExceededException.class)
                .satisfies(ex -> {
                    RefreshMemoryQuotaExceededException oom =
                            (RefreshMemoryQuotaExceededException) ex;
                    assertThat(oom.requested()).isEqualTo(1024);
                    assertThat(oom.available()).isEqualTo(512);
                });
    }

    @Test
    void releaseFreesBytes_allowsSubsequentAllocation() {
        BoundedRefreshMemoryQuota quota = new BoundedRefreshMemoryQuota(1024);
        quota.allocate(512);

        quota.release(512);

        assertThat(quota.usedBytes()).isZero();
        // Re-allocate full quota after release
        quota.allocate(1024);
        assertThat(quota.usedBytes()).isEqualTo(1024);
    }
}
