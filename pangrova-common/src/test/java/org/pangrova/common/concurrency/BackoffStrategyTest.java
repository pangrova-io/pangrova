package org.pangrova.common.concurrency;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for {@link BackoffStrategy}.
 * Covers exponential growth sequence, maxMs cap, reset behavior,
 * and poll/retry static factories.
 *
 * <p>Validates: Requirements 6.1-6.7
 */
class BackoffStrategyTest {

    // ── next() exponential growth sequence ──

    @Test
    void next_returnsExponentialSequence() {
        var strategy = new BackoffStrategy(1, 16);

        assertThat(strategy.next()).isEqualTo(1);
        assertThat(strategy.next()).isEqualTo(2);
        assertThat(strategy.next()).isEqualTo(4);
        assertThat(strategy.next()).isEqualTo(8);
        assertThat(strategy.next()).isEqualTo(16);
    }

    // ── next() caps at maxMs ──

    @Test
    void next_capsAtMaxMs() {
        var strategy = new BackoffStrategy(1, 16);

        // Exhaust to cap
        for (int i = 0; i < 5; i++) {
            strategy.next();
        }

        // All subsequent calls should return maxMs
        assertThat(strategy.next()).isEqualTo(16);
        assertThat(strategy.next()).isEqualTo(16);
        assertThat(strategy.next()).isEqualTo(16);
    }

    // ── reset() resets to initialMs ──

    @Test
    void reset_resetsToInitialMs() {
        var strategy = new BackoffStrategy(1, 16);

        strategy.next(); // 1
        strategy.next(); // 2
        strategy.next(); // 4

        strategy.reset();

        assertThat(strategy.next()).isEqualTo(1);
    }

    @Test
    void reset_allowsFullSequenceAgain() {
        var strategy = new BackoffStrategy(1, 16);

        // Run through full sequence
        for (int i = 0; i < 6; i++) {
            strategy.next();
        }

        strategy.reset();

        assertThat(strategy.next()).isEqualTo(1);
        assertThat(strategy.next()).isEqualTo(2);
        assertThat(strategy.next()).isEqualTo(4);
    }

    // ── poll() factory ──

    @Test
    void poll_createsStrategyWithInitialMs1() {
        var strategy = BackoffStrategy.poll(16);

        assertThat(strategy.next()).isEqualTo(1);
        assertThat(strategy.next()).isEqualTo(2);
        assertThat(strategy.next()).isEqualTo(4);
        assertThat(strategy.next()).isEqualTo(8);
        assertThat(strategy.next()).isEqualTo(16);
        assertThat(strategy.next()).isEqualTo(16);
    }

    // ── retry() factory ──

    @Test
    void retry_createsStrategyWithCorrectParams() {
        var strategy = BackoffStrategy.retry(1000, 30000);

        assertThat(strategy.next()).isEqualTo(1000);
        assertThat(strategy.next()).isEqualTo(2000);
        assertThat(strategy.next()).isEqualTo(4000);
        assertThat(strategy.next()).isEqualTo(8000);
        assertThat(strategy.next()).isEqualTo(16000);
        assertThat(strategy.next()).isEqualTo(30000);
        assertThat(strategy.next()).isEqualTo(30000);
    }

    // ── initialMs equals maxMs ──

    @Test
    void next_whenInitialEqualsMax_alwaysReturnsSameValue() {
        var strategy = new BackoffStrategy(5, 5);

        assertThat(strategy.next()).isEqualTo(5);
        assertThat(strategy.next()).isEqualTo(5);
        assertThat(strategy.next()).isEqualTo(5);
    }
}
