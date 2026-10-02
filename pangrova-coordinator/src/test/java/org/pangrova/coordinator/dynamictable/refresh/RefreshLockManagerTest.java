package org.pangrova.coordinator.dynamictable.refresh;

import org.junit.jupiter.api.Test;
import org.pangrova.coordinator.coordination.inmemory.InMemoryDistributedLease;
import org.pangrova.coordinator.dynamictable.refresh.lock.RefreshLockManager;
import org.pangrova.spi.coordination.LeaseEntry;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

class RefreshLockManagerTest {

    private static final Duration LONG_TIMEOUT = Duration.ofMinutes(10);
    private static final String DT = "sales_summary";

    @Test
    void acquireAndReleaseUseBackendIssuedToken() {
        RefreshLockManager manager = manager();

        LeaseEntry held = manager.tryAcquire(DT, "coord-1", LONG_TIMEOUT).orElseThrow();
        assertThat(held.epoch()).isEqualTo(1);
        assertThat(manager.lockCount()).hasValue(1);

        manager.release(held);
        assertThat(manager.lockCount()).hasValue(0);
        assertThat(manager.getLock(DT)).isNull();
    }

    @Test
    void activeHolderCannotBePreempted() {
        RefreshLockManager manager = manager();
        LeaseEntry first = manager.tryAcquire(DT, "coord-1", LONG_TIMEOUT).orElseThrow();

        assertThat(manager.tryAcquire(DT, "coord-2", LONG_TIMEOUT)).isEmpty();
        assertThat(manager.getLock(DT).epoch()).isEqualTo(first.epoch());
        assertThat(manager.getLock(DT).coordinatorId()).isEqualTo("coord-1");
    }

    @Test
    void releaseAdvancesEpochOnNextAcquisition() {
        RefreshLockManager manager = manager();
        LeaseEntry first = manager.tryAcquire(DT, "coord-1", LONG_TIMEOUT).orElseThrow();
        manager.release(first);

        LeaseEntry second = manager.tryAcquire(DT, "coord-2", LONG_TIMEOUT).orElseThrow();
        assertThat(second.epoch()).isEqualTo(first.epoch() + 1);
    }

    @Test
    void staleTokenCannotRenewOrReleaseReplacement() {
        RefreshLockManager manager = manager();
        LeaseEntry first = manager.tryAcquire(DT, "coord-1", LONG_TIMEOUT).orElseThrow();
        manager.release(first);
        LeaseEntry second = manager.tryAcquire(DT, "coord-2", LONG_TIMEOUT).orElseThrow();

        assertThat(manager.tryRenew(first, LONG_TIMEOUT)).isFalse();
        manager.release(first);
        assertThat(manager.getLock(DT).epoch()).isEqualTo(second.epoch());
    }

    @Test
    void holderCanRenewExactToken() {
        RefreshLockManager manager = manager();
        LeaseEntry held = manager.tryAcquire(
                DT, "coord-1", Duration.ofSeconds(5)).orElseThrow();

        assertThat(manager.tryRenew(held, LONG_TIMEOUT)).isTrue();
        assertThat(manager.getLock(DT).coordinatorId()).isEqualTo("coord-1");
    }

    @Test
    void expiredLeaseCanBeTakenOverWithNextEpoch() {
        MutableClock clock = new MutableClock();
        RefreshLockManager manager = new RefreshLockManager(
                new InMemoryDistributedLease(clock));
        LeaseEntry first = manager.tryAcquire(
                DT, "coord-1", Duration.ofMillis(10)).orElseThrow();
        clock.advance(Duration.ofMillis(30));

        LeaseEntry second = manager.tryAcquire(DT, "coord-2", LONG_TIMEOUT).orElseThrow();
        assertThat(second.epoch()).isEqualTo(first.epoch() + 1);
        assertThat(second.holderId()).isEqualTo("coord-2");
    }

    @Test
    void concurrentAcquireHasExactlyOneWinner() throws InterruptedException {
        RefreshLockManager manager = manager();
        int threadCount = 10;
        CountDownLatch start = new CountDownLatch(1);
        CountDownLatch done = new CountDownLatch(threadCount);
        List<Boolean> results = Collections.synchronizedList(new ArrayList<>());
        ExecutorService executor = Executors.newFixedThreadPool(threadCount);
        try {
            for (int i = 0; i < threadCount; i++) {
                String coordinatorId = "coord-" + i;
                executor.submit(() -> {
                    try {
                        start.await();
                        results.add(manager.tryAcquire(
                                DT, coordinatorId, LONG_TIMEOUT).isPresent());
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    } finally {
                        done.countDown();
                    }
                });
            }
            start.countDown();
            assertThat(done.await(5, TimeUnit.SECONDS)).isTrue();
        } finally {
            executor.shutdownNow();
        }

        assertThat(results).containsOnlyOnce(true);
        assertThat(manager.lockCount()).hasValue(1);
    }

    @Test
    void differentResourcesAcquireIndependently() {
        RefreshLockManager manager = manager();

        assertThat(manager.tryAcquire("dt-a", "coord-1", LONG_TIMEOUT)).isPresent();
        assertThat(manager.tryAcquire("dt-b", "coord-1", LONG_TIMEOUT)).isPresent();
        assertThat(manager.tryAcquire("dt-c", "coord-2", LONG_TIMEOUT)).isPresent();
        assertThat(manager.lockCount()).hasValue(3);
    }

    private static RefreshLockManager manager() {
        return new RefreshLockManager(new InMemoryDistributedLease());
    }

    private static final class MutableClock extends Clock {
        private Instant instant = Instant.parse("2026-09-19T00:00:00Z");

        private void advance(Duration duration) {
            instant = instant.plus(duration);
        }

        @Override
        public ZoneId getZone() {
            return ZoneId.of("UTC");
        }

        @Override
        public Clock withZone(ZoneId zone) {
            return this;
        }

        @Override
        public Instant instant() {
            return instant;
        }
    }
}
