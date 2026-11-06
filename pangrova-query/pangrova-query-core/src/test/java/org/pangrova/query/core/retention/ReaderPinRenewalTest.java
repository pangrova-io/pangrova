package org.pangrova.query.core.retention;

import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import org.junit.jupiter.api.Test;
import org.pangrova.query.spi.retention.ReaderPinLifecycle;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

class ReaderPinRenewalTest {
    @Test
    void providerMustSupplyAPositiveInterval() {
        for (long interval : new long[]{0, -1}) {
            assertThatThrownBy(() -> new ReaderPinRenewal(port(interval, ignored -> {}), List::of, () -> 0))
                    .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("positive");
        }
    }

    @Test
    void failedRenewalRetriesOnNextProviderInterval() {
        var clock = new AtomicLong();
        var attempts = new AtomicInteger();
        try (var renewal = new ReaderPinRenewal(port(10, ids -> {
            assertThat(ids).containsExactly("query");
            if (attempts.incrementAndGet() == 1) throw new IllegalStateException("storage unavailable");
        }), () -> List.of("query"), clock::get)) {
            renewal.renewDue();
            clock.set(9);
            renewal.renewDue();
            assertThat(attempts).hasValue(1);
            clock.set(10);
            renewal.renewDue();
            assertThat(attempts).hasValue(2);
        }
    }

    @Test
    void timerNeverRunsBlockingStorageAndCloseDoesNotCloseBorrowedExecutor() throws Exception {
        var entered = new CountDownLatch(1);
        var release = new CountDownLatch(1);
        var timer = Executors.newSingleThreadScheduledExecutor();
        var worker = Executors.newSingleThreadExecutor();
        var renewal = new ReaderPinRenewal(port(TimeUnit.MILLISECONDS.toNanos(10), ids -> {
            entered.countDown();
            try {
                assertThat(release.await(3, TimeUnit.SECONDS)).isTrue();
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException(interrupted);
            }
        }), () -> List.of("draining-query"), System::nanoTime);
        try {
            renewal.start(timer, worker);
            assertThat(entered.await(3, TimeUnit.SECONDS)).isTrue();
            // A database-blocked renewal cannot stop heartbeat/cleanup timer dispatch.
            assertThat(timer.submit(() -> "timer is responsive").get(1, TimeUnit.SECONDS))
                    .isEqualTo("timer is responsive");
            renewal.close();
            release.countDown();
            assertThat(worker.submit(() -> "still borrowed").get(3, TimeUnit.SECONDS))
                    .isEqualTo("still borrowed");
            assertThatThrownBy(() -> renewal.start(timer, worker)).isInstanceOf(IllegalStateException.class);
        } finally {
            release.countDown();
            renewal.close();
            timer.shutdownNow();
            worker.shutdownNow();
            assertThat(timer.awaitTermination(3, TimeUnit.SECONDS)).isTrue();
            assertThat(worker.awaitTermination(3, TimeUnit.SECONDS)).isTrue();
        }
    }

    @Test
    void onlyOneRenewalCanQueueAndQueuedWorkCannotRunAfterClose() throws Exception {
        var queued = new LinkedBlockingQueue<Runnable>();
        var accepted = new AtomicInteger();
        var renewed = new AtomicInteger();
        var timer = Executors.newSingleThreadScheduledExecutor();
        var renewal = new ReaderPinRenewal(port(TimeUnit.MILLISECONDS.toNanos(1), ids -> renewed.incrementAndGet()),
                () -> List.of("query"), System::nanoTime);
        try {
            renewal.start(timer, work -> { accepted.incrementAndGet(); queued.add(work); });
            Runnable first = queued.poll(3, TimeUnit.SECONDS);
            assertThat(first).isNotNull();
            timer.schedule(() -> {}, 30, TimeUnit.MILLISECONDS).get(3, TimeUnit.SECONDS);
            assertThat(accepted).hasValue(1);
            assertThat(queued).isEmpty();
            renewal.close();
            first.run();
            renewal.renewDue();
            assertThat(renewed).hasValue(0);
        } finally {
            renewal.close();
            timer.shutdownNow();
            assertThat(timer.awaitTermination(3, TimeUnit.SECONDS)).isTrue();
        }
    }

    @Test
    void rejectedDispatchDoesNotPermanentlyStopRenewal() throws Exception {
        var submissions = new AtomicInteger();
        var renewals = new AtomicInteger();
        var timer = Executors.newSingleThreadScheduledExecutor();
        var worker = Executors.newSingleThreadExecutor();
        var renewal = new ReaderPinRenewal(port(TimeUnit.MILLISECONDS.toNanos(10), ids -> renewals.incrementAndGet()),
                () -> List.of("query"), System::nanoTime);
        try {
            renewal.start(timer, work -> {
                if (submissions.incrementAndGet() == 1) throw new RejectedExecutionException("bounded queue full");
                worker.execute(work);
            });
            await().atMost(Duration.ofSeconds(3)).untilAsserted(() -> assertThat(renewals.get()).isPositive());
            assertThat(submissions.get()).isGreaterThanOrEqualTo(2);
        } finally {
            renewal.close();
            timer.shutdownNow();
            worker.shutdownNow();
            assertThat(timer.awaitTermination(3, TimeUnit.SECONDS)).isTrue();
            assertThat(worker.awaitTermination(3, TimeUnit.SECONDS)).isTrue();
        }
    }

    private static ReaderPinLifecycle port(long interval, Consumer<Collection<String>> renew) {
        return new ReaderPinLifecycle() {
            @Override public void release(String queryId) { }
            @Override public void renew(Collection<String> queryIds) { renew.accept(queryIds); }
            @Override public long renewIntervalNanos() { return interval; }
        };
    }
}
