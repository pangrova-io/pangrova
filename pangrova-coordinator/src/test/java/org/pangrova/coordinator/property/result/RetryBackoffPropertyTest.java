package org.pangrova.coordinator.property.result;

import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.constraints.IntRange;
import org.pangrova.common.concurrent.BackoffStrategy;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Property test: Retry with exponential backoff (Property 8).
 * Validates that BackoffStrategy produces monotonically non-decreasing delays
 * bounded by maxDelay.
 */
class RetryBackoffPropertyTest {

    @Property(tries = 200)
    void delaySequenceIsMonotonicallyNonDecreasing(
            @ForAll @IntRange(min = 10, max = 1000) int initialDelayMs,
            @ForAll @IntRange(min = 1, max = 10) int steps) {
        long maxDelay = initialDelayMs * 100L;
        BackoffStrategy backoff = BackoffStrategy.retry(initialDelayMs, maxDelay);
        long prev = 0;
        for (int i = 0; i < steps; i++) {
            long delay = backoff.next();
            assertThat(delay).isGreaterThanOrEqualTo(prev);
            prev = delay;
        }
    }

    @Property(tries = 200)
    void delayNeverExceedsMaxDelay(
            @ForAll @IntRange(min = 10, max = 500) int initialDelayMs,
            @ForAll @IntRange(min = 1, max = 20) int steps) {
        long maxDelay = initialDelayMs * 10L;
        BackoffStrategy backoff = BackoffStrategy.retry(initialDelayMs, maxDelay);
        for (int i = 0; i < steps; i++) {
            long delay = backoff.next();
            assertThat(delay).isLessThanOrEqualTo(maxDelay);
        }
    }

    @Property(tries = 200)
    void firstDelayEqualsInitialDelay(
            @ForAll @IntRange(min = 10, max = 5000) int initialDelayMs) {
        long maxDelay = initialDelayMs * 100L;
        BackoffStrategy backoff = BackoffStrategy.retry(initialDelayMs, maxDelay);
        long first = backoff.next();
        assertThat(first).isEqualTo(initialDelayMs);
    }
}
