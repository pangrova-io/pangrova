package org.pangrova.common.util;

import java.time.Duration;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for {@link Stopwatch}.
 * Covers: start() non-null, elapsedMs/elapsedNanos >= 0 immediately,
 * elapsed after sleep > 0, elapsed() returns Duration.
 *
 * <p>Validates: Requirements 11.1-11.5
 */
class StopwatchTest {

    // ── start() factory (Requirement 11.2) ──

    @Test
    void start_returnsNonNullStopwatch() {
        Stopwatch sw = Stopwatch.start();
        assertThat(sw).isNotNull();
    }

    // ── elapsedMs() immediately (Requirement 11.3) ──

    @Test
    void elapsedMs_immediatelyAfterStart_isNonNegative() {
        Stopwatch sw = Stopwatch.start();
        assertThat(sw.elapsedMs()).isGreaterThanOrEqualTo(0L);
    }

    // ── elapsedMs() after sleep (Requirement 11.3) ──

    @Test
    void elapsedMs_afterSleep_isPositive() throws InterruptedException {
        Stopwatch sw = Stopwatch.start();
        Thread.sleep(20);
        assertThat(sw.elapsedMs()).isGreaterThan(0L);
    }

    // ── elapsedNanos() immediately (Requirement 11.4) ──

    @Test
    void elapsedNanos_immediatelyAfterStart_isNonNegative() {
        Stopwatch sw = Stopwatch.start();
        assertThat(sw.elapsedNanos()).isGreaterThanOrEqualTo(0L);
    }

    // ── elapsed() returns Duration (Requirement 11.5) ──

    @Test
    void elapsed_returnsNonNullDuration() {
        Stopwatch sw = Stopwatch.start();
        Duration d = sw.elapsed();
        assertThat(d).isNotNull();
    }

    @Test
    void elapsed_toMillis_isNonNegative() {
        Stopwatch sw = Stopwatch.start();
        assertThat(sw.elapsed().toMillis()).isGreaterThanOrEqualTo(0L);
    }

    @Test
    void elapsed_afterSleep_toMillis_isPositive() throws InterruptedException {
        Stopwatch sw = Stopwatch.start();
        Thread.sleep(20);
        assertThat(sw.elapsed().toMillis()).isGreaterThan(0L);
    }
}
