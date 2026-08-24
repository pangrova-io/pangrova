package org.pangrova.resilience.property.breaker;

import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.constraints.IntRange;
import org.pangrova.resilience.breaker.CircuitBreaker;
import org.pangrova.resilience.breaker.CircuitBreakerConfig;

import java.time.Clock;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Property tests for {@link CircuitBreaker} state machine invariants.
 */
class CircuitBreakerStatePropertyTest {

    @Property(tries = 10)
    void closedBreaker_neverOpens_belowThreshold(
            @ForAll @IntRange(min = 10, max = 50) int minimumCalls) {

        CircuitBreaker breaker = new CircuitBreaker(
                new CircuitBreakerConfig(0.5, minimumCalls, 5000, 3, Clock.systemUTC()));

        // Record minimumCalls - 1 failures → should stay CLOSED
        for (int i = 0; i < minimumCalls - 1; i++) {
            breaker.onFailure();
        }
        assertThat(breaker.allowRequest()).isTrue();
    }

    @Property(tries = 10)
    void allFailures_aboveMinimum_opensBreaker(
            @ForAll @IntRange(min = 5, max = 30) int minimumCalls) {

        CircuitBreaker breaker = new CircuitBreaker(
                new CircuitBreakerConfig(0.5, minimumCalls, 60000, 3, Clock.systemUTC()));

        // Record enough failures to exceed threshold
        for (int i = 0; i < minimumCalls + 1; i++) {
            breaker.onFailure();
        }
        // Should be OPEN now
        assertThat(breaker.allowRequest()).isFalse();
    }

    @Property(tries = 10)
    void mixedSuccessAndFailure_staysClosedBelowRate(
            @ForAll @IntRange(min = 10, max = 50) int minimumCalls) {

        // 50% threshold, but we'll only fail 30% of calls
        CircuitBreaker breaker = new CircuitBreaker(
                new CircuitBreakerConfig(0.5, minimumCalls, 60000, 3, Clock.systemUTC()));

        int failCount = (int) (minimumCalls * 0.3);
        int successCount = minimumCalls - failCount;

        for (int i = 0; i < successCount; i++) {
            breaker.onSuccess();
        }
        for (int i = 0; i < failCount; i++) {
            breaker.onFailure();
        }
        // 30% failure rate < 50% threshold → should stay CLOSED
        assertThat(breaker.allowRequest()).isTrue();
    }
}
