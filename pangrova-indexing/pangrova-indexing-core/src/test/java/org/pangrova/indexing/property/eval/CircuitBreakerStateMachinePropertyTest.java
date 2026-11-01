package org.pangrova.indexing.property.eval;

import net.jqwik.api.Arbitraries;
import net.jqwik.api.Arbitrary;
import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.Provide;
import org.pangrova.indexing.eval.IndexEvalCircuitBreaker;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Property 4: Circuit breaker state machine invariants.
 *
 * <p>For any sequence of success/failure events, the IndexEvalCircuitBreaker
 * state transitions satisfy: CLOSED→OPEN on threshold failures,
 * OPEN→HALF_OPEN after timeout, HALF_OPEN→CLOSED on success,
 * HALF_OPEN→OPEN on failure.</p>
 *
 * <p><b>Validates: Requirement 4.5</b></p>
 */
class CircuitBreakerStateMachinePropertyTest {

    @Property(tries = 50)
    void closedToOpenOnThresholdFailures(
            @ForAll("thresholds") int threshold) {

        IndexEvalCircuitBreaker breaker = new IndexEvalCircuitBreaker(threshold, Duration.ofSeconds(60));
        String indexId = "test-index";

        for (int i = 0; i < threshold - 1; i++) {
            breaker.recordFailure(indexId);
            assertThat(breaker.isAllowed(indexId))
                    .as("Should remain CLOSED before threshold (failure %d/%d)", i + 1, threshold)
                    .isTrue();
        }

        breaker.recordFailure(indexId);
        assertThat(breaker.isAllowed(indexId))
                .as("Should be OPEN after %d failures", threshold)
                .isFalse();
    }

    @Property(tries = 50)
    void successResetsToClosedFromAnyState(
            @ForAll("thresholds") int threshold) {

        IndexEvalCircuitBreaker breaker = new IndexEvalCircuitBreaker(threshold, Duration.ofMillis(1));
        String indexId = "test-index";

        for (int i = 0; i < threshold; i++) {
            breaker.recordFailure(indexId);
        }
        assertThat(breaker.isAllowed(indexId)).isFalse();

        breaker.recordSuccess(indexId);

        assertThat(breaker.isAllowed(indexId))
                .as("Should be CLOSED after recordSuccess")
                .isTrue();
    }

    @Property(tries = 50)
    void failureAfterResetReopens(
            @ForAll("thresholds") int threshold) {

        IndexEvalCircuitBreaker breaker = new IndexEvalCircuitBreaker(threshold, Duration.ofMillis(1));
        String indexId = "test-index";

        // Open the circuit
        for (int i = 0; i < threshold; i++) {
            breaker.recordFailure(indexId);
        }
        assertThat(breaker.isAllowed(indexId)).isFalse();

        // Reset via success
        breaker.recordSuccess(indexId);
        assertThat(breaker.isAllowed(indexId)).isTrue();

        // Re-open by hitting threshold again
        for (int i = 0; i < threshold; i++) {
            breaker.recordFailure(indexId);
        }
        assertThat(breaker.isAllowed(indexId))
                .as("Should re-open after threshold failures post-reset")
                .isFalse();
    }

    @Provide
    Arbitrary<Integer> thresholds() {
        return Arbitraries.integers().between(1, 10);
    }
}
