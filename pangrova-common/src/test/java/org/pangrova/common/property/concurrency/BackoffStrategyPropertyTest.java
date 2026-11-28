package org.pangrova.common.property.concurrency;

import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.constraints.LongRange;
import org.pangrova.common.concurrency.BackoffStrategy;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Property tests for {@link BackoffStrategy}.
 *
 * <p>Verifies three universal properties:
 * <ol>
 *   <li>{@code next()} return value is always &lt;= maxMs</li>
 *   <li>{@code next()} return value on first call is always &gt;= initialMs</li>
 *   <li>{@code reset()} followed by {@code next()} always returns initialMs</li>
 * </ol>
 *
 * <p><b>Validates: Requirements 6.1-6.7</b></p>
 */
class BackoffStrategyPropertyTest {

    /**
     * Property 1: next() return value is always &lt;= maxMs.
     *
     * <p>For arbitrary initialMs and maxMs (with initialMs &lt;= maxMs),
     * calling next() any number of times must never exceed maxMs.
     * We call next() up to 30 times to cover well beyond the doubling saturation point.
     */
    @Property(tries = 10)
    void nextNeverExceedsMaxMs(
            @ForAll @LongRange(min = 1, max = 10_000) long initialMs,
            @ForAll @LongRange(min = 1, max = 100_000) long maxMs) {
        long effectiveMax = Math.max(initialMs, maxMs);
        var strategy = new BackoffStrategy(initialMs, effectiveMax);

        for (int i = 0; i < 30; i++) {
            long value = strategy.next();
            assertThat(value)
                    .as("next() call %d should be <= maxMs (%d)", i, effectiveMax)
                    .isLessThanOrEqualTo(effectiveMax);
        }
    }

    /**
     * Property 2: first next() call always returns initialMs.
     *
     * <p>For arbitrary initialMs and maxMs (with initialMs &lt;= maxMs),
     * the very first call to next() must return exactly initialMs.
     */
    @Property(tries = 10)
    void firstNextReturnsInitialMs(
            @ForAll @LongRange(min = 1, max = 10_000) long initialMs,
            @ForAll @LongRange(min = 1, max = 100_000) long maxMs) {
        long effectiveMax = Math.max(initialMs, maxMs);
        var strategy = new BackoffStrategy(initialMs, effectiveMax);

        assertThat(strategy.next())
                .as("first next() should return initialMs (%d)", initialMs)
                .isEqualTo(initialMs);
    }

    /**
     * Property 3: reset followed by next() always returns initialMs.
     *
     * <p>For arbitrary initialMs and maxMs (with initialMs &lt;= maxMs),
     * after calling next() several times and then reset(), the next call
     * to next() must return exactly initialMs.
     */
    @Property(tries = 10)
    void resetFollowedByNextReturnsInitialMs(
            @ForAll @LongRange(min = 1, max = 10_000) long initialMs,
            @ForAll @LongRange(min = 1, max = 100_000) long maxMs,
            @ForAll @LongRange(min = 1, max = 20) long advanceCalls) {
        long effectiveMax = Math.max(initialMs, maxMs);
        var strategy = new BackoffStrategy(initialMs, effectiveMax);

        // Advance the strategy a random number of times
        for (long i = 0; i < advanceCalls; i++) {
            strategy.next();
        }

        strategy.reset();

        assertThat(strategy.next())
                .as("next() after reset() should return initialMs (%d)", initialMs)
                .isEqualTo(initialMs);
    }
}
