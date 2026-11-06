package org.pangrova.query.core.lifecycle;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;
import org.pangrova.common.concurrent.ExecutorDrain;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

class QueryTimeoutSchedulerTest {
    private static final Instant NOW = Instant.parse("2026-01-01T00:00:00Z");
    private static final Duration TIMEOUT = Duration.ofSeconds(30);

    private static QueryTimeoutScheduler create(ScheduledThreadPoolExecutor executor) {
        return new QueryTimeoutScheduler(TIMEOUT, Duration.ofMillis(10), executor,
                Clock.fixed(NOW, ZoneOffset.UTC));
    }

    @Test
    void passesInclusiveDeadlineAndConfiguredDurationToOwner() throws Exception {
        var scheduler = create(new ScheduledThreadPoolExecutor(1));
        var called = new CountDownLatch(1);
        var deadline = new AtomicReference<Instant>();
        var duration = new AtomicReference<Duration>();
        try {
            scheduler.start((cutoff, timeout) -> {
                deadline.set(cutoff);
                duration.set(timeout);
                called.countDown();
            });
            assertThat(called.await(3, TimeUnit.SECONDS)).isTrue();
            assertThat(deadline.get()).isEqualTo(NOW.minus(TIMEOUT));
            assertThat(duration.get()).isEqualTo(TIMEOUT);
        } finally {
            scheduler.stop();
        }
    }

    @Test
    void ownerFailureIsReportedWithoutSuppressingLaterSweeps() throws Exception {
        var scheduler = create(new ScheduledThreadPoolExecutor(1));
        var calls = new AtomicInteger();
        var recovered = new CountDownLatch(1);
        try {
            scheduler.start((deadline, timeout) -> {
                if (calls.incrementAndGet() == 1) throw new IllegalStateException("Owner unavailable");
                recovered.countDown();
            });
            assertThat(recovered.await(3, TimeUnit.SECONDS)).isTrue();
            assertThat(calls.get()).isGreaterThanOrEqualTo(2);
        } finally {
            scheduler.stop();
        }
    }

    @Test
    void stopWaitsForActualCallbackExitAndPreventsFurtherSweeps() throws Exception {
        var executor = new ScheduledThreadPoolExecutor(1);
        var scheduler = create(executor);
        var entered = new CountDownLatch(1);
        var release = new CountDownLatch(1);
        var stopper = Executors.newSingleThreadExecutor();
        var calls = new AtomicInteger();
        try {
            scheduler.start((deadline, timeout) -> {
                calls.incrementAndGet();
                entered.countDown();
                try {
                    if (!release.await(3, TimeUnit.SECONDS)) throw new IllegalStateException("Callback not released");
                } catch (InterruptedException failure) {
                    Thread.currentThread().interrupt();
                    throw new IllegalStateException(failure);
                }
            });
            assertThat(entered.await(3, TimeUnit.SECONDS)).isTrue();
            var stopped = stopper.submit(scheduler::stop);
            await().atMost(Duration.ofSeconds(3)).until(executor::isShutdown);
            assertThat(stopped.isDone()).isFalse();
            release.countDown();
            stopped.get(3, TimeUnit.SECONDS);
            assertThat(executor.isTerminated()).isTrue();
            assertThat(calls).hasValue(1);
            scheduler.stop();
        } finally {
            release.countDown();
            scheduler.stop();
            ExecutorDrain.shutdownAndAwait(stopper, 3000);
        }
    }

    @Test
    void ownerIsMandatoryAndCannotBeReplacedAfterStart() {
        var executor = new ScheduledThreadPoolExecutor(1);
        var scheduler = create(executor);
        try {
            assertThatThrownBy(() -> scheduler.start(null)).isInstanceOf(NullPointerException.class);
            assertThat(executor.getQueue()).isEmpty();
            scheduler.start((deadline, timeout) -> { });
            assertThatThrownBy(() -> scheduler.start((deadline, timeout) -> { }))
                    .isInstanceOf(IllegalStateException.class).hasMessageContaining("already started");
        } finally {
            scheduler.stop();
        }
    }
}
