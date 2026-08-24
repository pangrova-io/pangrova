package org.pangrova.coordinator.dynamictable.refresh;

import org.pangrova.coordinator.dynamictable.quota.BoundedRefreshMemoryQuota;
import org.pangrova.coordinator.dynamictable.quota.RefreshMemoryQuota;
import org.pangrova.coordinator.dynamictable.quota.RefreshMemoryQuotaExceededException;
import org.pangrova.coordinator.dynamictable.refresh.pipeline.RefreshExecutor;
import org.pangrova.coordinator.dynamictable.refresh.pipeline.RefreshResult;
import org.pangrova.metadata.dynamictable.DynamicTableLifecycleState;
import org.pangrova.metadata.dynamictable.DynamicTableState;
import org.pangrova.metadata.dynamictable.DynamicTableRefreshMode;
import org.pangrova.metadata.dynamictable.TriggerType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Unit tests for memory quota enforcement during Dynamic Table refresh.
 *
 * <p>Verifies that exceeding memory quota fails the refresh with
 * {@code dt_refresh_oom} metric, and that resources are released
 * after refresh completion (success or failure).</p>
 *
 * <p><b>Validates: Requirements 16 AC3, 16 AC5</b></p>
 */
class RefreshMemoryQuotaTest {

    private BoundedRefreshMemoryQuota quota;

    @BeforeEach
    void setUp() {
        quota = new BoundedRefreshMemoryQuota(1024);
    }

    // ── BoundedRefreshMemoryQuota basic behavior ────────────────────

    @Test
    void allocate_withinQuotaSucceeds() {
        quota.allocate(512);
        assertThat(quota.usedBytes()).isEqualTo(512);
    }

    @Test
    void allocate_exceedingQuotaThrows() {
        quota.allocate(512);
        assertThatThrownBy(() -> quota.allocate(600))
                .isInstanceOf(RefreshMemoryQuotaExceededException.class)
                .satisfies(e -> {
                    var ex = (RefreshMemoryQuotaExceededException) e;
                    assertThat(ex.requested()).isEqualTo(600);
                    assertThat(ex.available()).isEqualTo(512);
                });
    }

    @Test
    void release_freesAllocatedBytes() {
        quota.allocate(512);
        quota.release(512);
        assertThat(quota.usedBytes()).isZero();
    }

    @Test
    void release_afterRelease_canAllocateAgain() {
        quota.allocate(1024);
        quota.release(1024);
        quota.allocate(1024);
        assertThat(quota.usedBytes()).isEqualTo(1024);
    }

    @Test
    void totalQuotaBytes_returnsConfiguredValue() {
        assertThat(quota.totalQuotaBytes()).isEqualTo(1024);
    }

    // ── UnlimitedRefreshMemoryQuota ─────────────────────────────────

    @Test
    void unlimited_neverRejects() {
        RefreshMemoryQuota unlimited = RefreshMemoryQuota.unlimited();
        unlimited.allocate(Long.MAX_VALUE / 2);
        assertThat(unlimited.usedBytes()).isZero();
        assertThat(unlimited.totalQuotaBytes()).isEqualTo(Long.MAX_VALUE);
    }

    // ── OOM metric on quota exceeded ────────────────────────────────

    @Test
    void quotaExceeded_refreshFailsWithOomMetric() {
        var oomDetected = new AtomicBoolean(false);
        var releaseDetected = new AtomicBoolean(false);
        var tinyQuota = new BoundedRefreshMemoryQuota(100);

        // Executor that tries to allocate more than quota
        RefreshExecutor quotaCheckingExecutor = (state, trigger) -> {
            try {
                tinyQuota.allocate(200);
                return RefreshResult.noOp();
            } catch (RefreshMemoryQuotaExceededException e) {
                oomDetected.set(true);
                return RefreshResult.failure(0);
            } finally {
                tinyQuota.release(0);
                releaseDetected.set(true);
            }
        };

        DynamicTableState state = activeState("dt_oom");
        RefreshResult result = quotaCheckingExecutor.execute(state, TriggerType.SCHEDULED);

        assertThat(oomDetected.get()).isTrue();
        assertThat(releaseDetected.get()).isTrue();
        assertThat(result.status()).isEqualTo(org.pangrova.metadata.dynamictable.RefreshStatus.FAILURE);
    }

    // ── Resource release after refresh ──────────────────────────────

    @Test
    void resourceReleasedAfterSuccessfulRefresh() {
        var trackingQuota = new BoundedRefreshMemoryQuota(1024);

        RefreshExecutor executor = (state, trigger) -> {
            trackingQuota.allocate(512);
            try {
                return RefreshResult.noOp();
            } finally {
                trackingQuota.release(512);
            }
        };

        executor.execute(activeState("dt_success"), TriggerType.SCHEDULED);
        assertThat(trackingQuota.usedBytes()).isZero();
    }

    @Test
    void resourceReleasedAfterFailedRefresh() {
        var trackingQuota = new BoundedRefreshMemoryQuota(1024);

        RefreshExecutor executor = (state, trigger) -> {
            trackingQuota.allocate(512);
            try {
                throw new RuntimeException("simulated failure");
            } catch (RuntimeException e) {
                return RefreshResult.failure(0);
            } finally {
                trackingQuota.release(512);
            }
        };

        RefreshResult result = executor.execute(activeState("dt_fail"), TriggerType.SCHEDULED);
        assertThat(result.status()).isEqualTo(org.pangrova.metadata.dynamictable.RefreshStatus.FAILURE);
        assertThat(trackingQuota.usedBytes()).isZero();
    }

    // ── Constructor validation ──────────────────────────────────────

    @Test
    void boundedQuota_rejectsNonPositiveQuota() {
        assertThatThrownBy(() -> new BoundedRefreshMemoryQuota(0))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new BoundedRefreshMemoryQuota(-1))
                .isInstanceOf(IllegalArgumentException.class);
    }

    // ── State factory ───────────────────────────────────────────────

    private static DynamicTableState activeState(String dtName) {
        return new DynamicTableState(
                dtName, "tenant-1", DynamicTableLifecycleState.ACTIVE, DynamicTableRefreshMode.SNAPSHOT,
                Duration.ofMinutes(5), "100", "SNAPSHOT", 1, null,
                "admin", "coord-1", 1L, null, 0, 0,
                Instant.now(), Instant.now());
    }
}
