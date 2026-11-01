package org.pangrova.persistence.memory.coordination;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

class InMemoryLeaseContentionTest {
    @ParameterizedTest
    @ValueSource(strings = {"renew", "release"})
    void expiredLeaseIsNotMutatedUsingTimeObservedBeforeWaiting(String operation) throws Exception {
        var clock = new GateClock();
        var leases = new InMemoryDistributedLease(clock);
        var lease = leases.tryAcquire("key", "owner", Duration.ofSeconds(10)).orElseThrow();
        clock.blockNext.set(true);
        var holderTask = new FutureTask<>(() -> leases.tryAcquire("key", "owner", Duration.ofSeconds(10)));
        var waiterTask = new FutureTask<>(() -> operation.equals("renew")
                ? leases.tryRenew("key", "owner", lease.epoch(), Duration.ofSeconds(60))
                : leases.release("key", "owner", lease.epoch()));
        var holder = new Thread(holderTask, "lease-map-owner");
        var waiter = new Thread(waiterTask, "lease-map-waiter");
        try {
            holder.start();
            assertThat(clock.entered.await(5, TimeUnit.SECONDS)).isTrue();
            waiter.start();
            await().atMost(Duration.ofSeconds(5)).until(() -> waiter.getState() == Thread.State.BLOCKED);
            clock.time.set(clock.time.get().plusSeconds(20));
            clock.release.countDown();
            holderTask.get(5, TimeUnit.SECONDS);
            assertThat(waiterTask.get(5, TimeUnit.SECONDS)).isFalse();
            assertThat(leases.currentHolder("key")).isEmpty();
            assertThat(leases.tryAcquire("key", "replacement", Duration.ofSeconds(10))).get()
                    .extracting(entry -> entry.epoch()).isEqualTo(lease.epoch() + 1);
        } finally {
            clock.release.countDown();
            holder.join(5000);
            waiter.join(5000);
            assertThat(holder.isAlive()).isFalse();
            assertThat(waiter.isAlive()).isFalse();
        }
    }

    /** Pauses one clock read while the corresponding acquire owns the map's per-key mutation lock. */
    private static final class GateClock extends Clock {
        private final AtomicReference<Instant> time =
                new AtomicReference<>(Instant.parse("2026-01-01T00:00:00Z"));
        private final AtomicBoolean blockNext = new AtomicBoolean();
        private final CountDownLatch entered = new CountDownLatch(1);
        private final CountDownLatch release = new CountDownLatch(1);

        @Override public ZoneId getZone() { return ZoneOffset.UTC; }
        @Override public Clock withZone(ZoneId zone) { return Clock.fixed(time.get(), zone); }
        @Override public Instant instant() {
            var observed = time.get();
            if (blockNext.compareAndSet(true, false)) {
                entered.countDown();
                try {
                    if (!release.await(5, TimeUnit.SECONDS)) throw new IllegalStateException("Clock gate timed out");
                } catch (InterruptedException failure) {
                    Thread.currentThread().interrupt();
                    throw new IllegalStateException("Clock gate interrupted", failure);
                }
            }
            return observed;
        }
    }
}
