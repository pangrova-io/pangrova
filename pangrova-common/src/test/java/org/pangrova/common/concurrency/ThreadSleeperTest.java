package org.pangrova.common.concurrency;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for {@link ThreadSleeper}.
 * Covers sleepQuietly(long) and sleepQuietly(Duration) for normal sleep,
 * non-positive input no-op, and interrupt flag restoration.
 */
class ThreadSleeperTest {

    // ── sleepQuietly(long) — positive value ──

    @Test
    void sleepQuietly_positiveDuration_actuallySleeps() {
        long start = System.nanoTime();

        ThreadSleeper.sleepQuietly(10);

        long elapsedMs = (System.nanoTime() - start) / 1_000_000;
        assertThat(elapsedMs).isGreaterThanOrEqualTo(5);
    }

    // ── sleepQuietly(long) — non-positive values ──

    @Test
    void sleepQuietly_zero_isNoOp() {
        long start = System.nanoTime();

        ThreadSleeper.sleepQuietly(0);

        long elapsedMs = (System.nanoTime() - start) / 1_000_000;
        assertThat(elapsedMs).isLessThan(5);
    }

    @Test
    void sleepQuietly_negative_isNoOp() {
        long start = System.nanoTime();

        ThreadSleeper.sleepQuietly(-1);

        long elapsedMs = (System.nanoTime() - start) / 1_000_000;
        assertThat(elapsedMs).isLessThan(5);
    }

    // ── sleepQuietly(Duration) — delegates correctly ──

    @Test
    void sleepQuietlyDuration_positiveDuration_actuallySleeps() {
        long start = System.nanoTime();

        ThreadSleeper.sleepQuietly(Duration.ofMillis(10));

        long elapsedMs = (System.nanoTime() - start) / 1_000_000;
        assertThat(elapsedMs).isGreaterThanOrEqualTo(5);
    }

    @Test
    void sleepQuietlyDuration_zeroDuration_isNoOp() {
        long start = System.nanoTime();

        ThreadSleeper.sleepQuietly(Duration.ZERO);

        long elapsedMs = (System.nanoTime() - start) / 1_000_000;
        assertThat(elapsedMs).isLessThan(5);
    }

    // ── Interrupt restoration ──

    @Test
    void sleepQuietly_restoresInterruptFlag_whenInterruptedDuringSleep() throws Exception {
        var interruptRestored = new AtomicBoolean(false);
        var thread = new Thread(() -> {
            Thread.currentThread().interrupt();
            ThreadSleeper.sleepQuietly(5_000);
            interruptRestored.set(Thread.currentThread().isInterrupted());
        });
        thread.start();
        thread.join(2_000);

        assertThat(interruptRestored).isTrue();
    }

    @Test
    void sleepQuietly_restoresInterruptFlag_whenInterruptedExternally() throws Exception {
        var interruptRestored = new AtomicBoolean(false);
        var sleeping = new AtomicBoolean(false);
        var thread = new Thread(() -> {
            sleeping.set(true);
            ThreadSleeper.sleepQuietly(5_000);
            interruptRestored.set(Thread.currentThread().isInterrupted());
        });
        thread.start();

        // Wait for the thread to enter sleep
        while (!sleeping.get()) {
            Thread.onSpinWait();
        }
        Thread.sleep(20); // small buffer to ensure Thread.sleep is entered
        thread.interrupt();
        thread.join(2_000);

        assertThat(interruptRestored).isTrue();
    }

    // ── Private constructor inaccessibility ──

    @Test
    void cannotInstantiate() throws Exception {
        var ctor = ThreadSleeper.class.getDeclaredConstructor();
        assertThat(ctor.canAccess(null)).isFalse();
    }
}
