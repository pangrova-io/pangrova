package org.pangrova.coordinator.dynamictable.refresh;

import org.pangrova.coordinator.coordination.inmemory.InMemoryDistributedLease;
import org.pangrova.coordinator.dynamictable.refresh.lock.RefreshLockManager;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for {@link RefreshLockManager}.
 *
 * <p>Covers: normal acquire/release, concurrent acquire mutual exclusion,
 * epoch fencing, and timeout expiration.</p>
 *
 * <p><b>Validates: Requirements 21</b></p>
 */
class RefreshLockManagerTest {

    private static final Duration LONG_TIMEOUT = Duration.ofMinutes(10);
    private static final String DT = "sales_summary";

    // ── Normal acquire / release ────────────────────────────────────

    @Test
    void acquireOnEmptyManager_succeeds() {
        RefreshLockManager manager = new RefreshLockManager(new InMemoryDistributedLease());

        boolean result = manager.tryAcquire(DT, "coord-1", 1L, LONG_TIMEOUT);

        assertThat(result).isTrue();
        assertThat(manager.lockCount()).hasValue(1);
    }

    @Test
    void releaseByHolder_removesLock() {
        RefreshLockManager manager = new RefreshLockManager(new InMemoryDistributedLease());
        manager.tryAcquire(DT, "coord-1", 1L, LONG_TIMEOUT);

        manager.release(DT, "coord-1");

        assertThat(manager.lockCount()).hasValue(0);
        assertThat(manager.getLock(DT)).isNull();
    }

    @Test
    void releaseByNonHolder_keepsLock() {
        RefreshLockManager manager = new RefreshLockManager(new InMemoryDistributedLease());
        manager.tryAcquire(DT, "coord-1", 1L, LONG_TIMEOUT);

        manager.release(DT, "coord-2");

        assertThat(manager.lockCount()).hasValue(1);
        assertThat(manager.getLock(DT).coordinatorId()).isEqualTo("coord-1");
    }

    @Test
    void releaseNonExistentLock_isNoOp() {
        RefreshLockManager manager = new RefreshLockManager(new InMemoryDistributedLease());

        manager.release("nonexistent", "coord-1");

        assertThat(manager.lockCount()).hasValue(0);
    }

    // ── Re-entrant acquire ──────────────────────────────────────────

    @Test
    void sameCoordinatorCanReacquire() {
        RefreshLockManager manager = new RefreshLockManager(new InMemoryDistributedLease());
        manager.tryAcquire(DT, "coord-1", 1L, LONG_TIMEOUT);

        boolean result = manager.tryAcquire(DT, "coord-1", 1L, LONG_TIMEOUT);

        assertThat(result).isTrue();
    }

    // ── Concurrent acquire mutual exclusion ─────────────────────────

    @Test
    void concurrentAcquire_onlyOneSucceeds() throws InterruptedException {
        RefreshLockManager manager = new RefreshLockManager(new InMemoryDistributedLease());
        int threadCount = 10;
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch doneLatch = new CountDownLatch(threadCount);
        List<Boolean> results = Collections.synchronizedList(new ArrayList<>());

        ExecutorService executor = Executors.newFixedThreadPool(threadCount);
        for (int i = 0; i < threadCount; i++) {
            String coordId = "coord-" + i;
            executor.submit(() -> {
                try {
                    startLatch.await();
                    boolean acquired = manager.tryAcquire(DT, coordId, 1L, LONG_TIMEOUT);
                    results.add(acquired);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } finally {
                    doneLatch.countDown();
                }
            });
        }

        startLatch.countDown();
        doneLatch.await(5, TimeUnit.SECONDS);
        executor.shutdown();

        long successCount = results.stream().filter(Boolean::booleanValue).count();
        assertThat(successCount).isEqualTo(1);
        assertThat(manager.lockCount()).hasValue(1);
    }

    // ── Epoch fencing ───────────────────────────────────────────────

    @Test
    void higherEpoch_forcesReplace() {
        RefreshLockManager manager = new RefreshLockManager(new InMemoryDistributedLease());
        manager.tryAcquire(DT, "old-leader", 1L, LONG_TIMEOUT);

        boolean result = manager.tryAcquire(DT, "new-leader", 2L, LONG_TIMEOUT);

        assertThat(result).isTrue();
        assertThat(manager.getLock(DT).coordinatorId()).isEqualTo("new-leader");
        assertThat(manager.getLock(DT).epoch()).isEqualTo(2L);
    }

    @Test
    void lowerEpoch_isRejected() {
        RefreshLockManager manager = new RefreshLockManager(new InMemoryDistributedLease());
        manager.tryAcquire(DT, "new-leader", 5L, LONG_TIMEOUT);

        boolean result = manager.tryAcquire(DT, "old-leader", 3L, LONG_TIMEOUT);

        assertThat(result).isFalse();
        assertThat(manager.getLock(DT).coordinatorId()).isEqualTo("new-leader");
    }

    @Test
    void sameEpochDifferentCoordinator_isRejected() {
        RefreshLockManager manager = new RefreshLockManager(new InMemoryDistributedLease());
        manager.tryAcquire(DT, "coord-1", 5L, LONG_TIMEOUT);

        boolean result = manager.tryAcquire(DT, "coord-2", 5L, LONG_TIMEOUT);

        assertThat(result).isFalse();
        assertThat(manager.getLock(DT).coordinatorId()).isEqualTo("coord-1");
    }

    // ── Timeout expiration ──────────────────────────────────────────

    @Test
    void expiredLock_canBeAcquiredByAnother() {
        RefreshLockManager manager = new RefreshLockManager(new InMemoryDistributedLease());
        manager.tryAcquire(DT, "coord-1", 1L, Duration.ZERO);

        // Lock is immediately expired, another coordinator can acquire
        boolean result = manager.tryAcquire(DT, "coord-2", 1L, LONG_TIMEOUT);

        assertThat(result).isTrue();
        assertThat(manager.getLock(DT).coordinatorId()).isEqualTo("coord-2");
    }

    @Test
    void expireStale_removesExpiredLocks() {
        RefreshLockManager manager = new RefreshLockManager(new InMemoryDistributedLease());
        manager.tryAcquire("dt-a", "coord-1", 1L, Duration.ZERO);
        manager.tryAcquire("dt-b", "coord-1", 1L, Duration.ZERO);
        manager.tryAcquire("dt-c", "coord-1", 1L, LONG_TIMEOUT);

        manager.expireStale();

        assertThat(manager.lockCount()).hasValue(1);
        assertThat(manager.getLock("dt-a")).isNull();
        assertThat(manager.getLock("dt-b")).isNull();
        assertThat(manager.getLock("dt-c")).isNotNull();
    }

    @Test
    void expireStale_onEmptyManager_isNoOp() {
        RefreshLockManager manager = new RefreshLockManager(new InMemoryDistributedLease());

        manager.expireStale();

        assertThat(manager.lockCount()).hasValue(0);
    }

    // ── Renewal (D21) ───────────────────────────────────────────────

    @Test
    void renewByHolder_succeeds() {
        RefreshLockManager manager = new RefreshLockManager(new InMemoryDistributedLease());
        manager.tryAcquire(DT, "coord-1", 1L, Duration.ofSeconds(5));

        boolean renewed = manager.tryRenew(DT, "coord-1", Duration.ofMinutes(10));

        assertThat(renewed).isTrue();
        assertThat(manager.getLock(DT)).isNotNull();
        assertThat(manager.getLock(DT).coordinatorId()).isEqualTo("coord-1");
    }

    @Test
    void renewByNonHolder_fails() {
        RefreshLockManager manager = new RefreshLockManager(new InMemoryDistributedLease());
        manager.tryAcquire(DT, "coord-1", 1L, LONG_TIMEOUT);

        boolean renewed = manager.tryRenew(DT, "coord-2", LONG_TIMEOUT);

        assertThat(renewed).isFalse();
        assertThat(manager.getLock(DT).coordinatorId()).isEqualTo("coord-1");
    }

    @Test
    void renewNonExistentLock_fails() {
        RefreshLockManager manager = new RefreshLockManager(new InMemoryDistributedLease());

        boolean renewed = manager.tryRenew("nonexistent", "coord-1", LONG_TIMEOUT);

        assertThat(renewed).isFalse();
    }

    @Test
    void renewExpiredLock_fails() {
        RefreshLockManager manager = new RefreshLockManager(new InMemoryDistributedLease());
        manager.tryAcquire(DT, "coord-1", 1L, Duration.ZERO);

        boolean renewed = manager.tryRenew(DT, "coord-1", LONG_TIMEOUT);

        assertThat(renewed).isFalse();
    }

    @Test
    void renewExtendsTtl_lockSurvivesExpireStale() {
        RefreshLockManager manager = new RefreshLockManager(new InMemoryDistributedLease());
        manager.tryAcquire(DT, "coord-1", 1L, Duration.ofSeconds(1));

        // Renew with a long timeout
        boolean renewed = manager.tryRenew(DT, "coord-1", LONG_TIMEOUT);
        assertThat(renewed).isTrue();

        // expireStale should NOT remove the renewed lock
        manager.expireStale();
        assertThat(manager.getLock(DT)).isNotNull();
        assertThat(manager.lockCount()).hasValue(1);
    }

    // ── Multiple DTs ────────────────────────────────────────────────

    @Test
    void differentDtNames_canBeAcquiredIndependently() {
        RefreshLockManager manager = new RefreshLockManager(new InMemoryDistributedLease());

        boolean a = manager.tryAcquire("dt-a", "coord-1", 1L, LONG_TIMEOUT);
        boolean b = manager.tryAcquire("dt-b", "coord-1", 1L, LONG_TIMEOUT);
        boolean c = manager.tryAcquire("dt-c", "coord-2", 1L, LONG_TIMEOUT);

        assertThat(a).isTrue();
        assertThat(b).isTrue();
        assertThat(c).isTrue();
        assertThat(manager.lockCount()).hasValue(3);
    }
}
