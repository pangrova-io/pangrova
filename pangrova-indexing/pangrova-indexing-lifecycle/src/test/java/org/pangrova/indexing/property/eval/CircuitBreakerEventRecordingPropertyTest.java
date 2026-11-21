package org.pangrova.indexing.property.eval;

import net.jqwik.api.Arbitraries;
import net.jqwik.api.Arbitrary;
import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.Provide;
import org.pangrova.indexing.evaluation.IndexEvalCircuitBreaker;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Property 6: Circuit breaker event recording consistency.
 *
 * <p>For any index evaluation, success records keep the circuit CLOSED,
 * and failure records drive it toward OPEN. The two are mutually exclusive
 * per evaluation — exactly one is called.</p>
 *
 * <p>Tests the observable effect of recordSuccess/recordFailure on breaker
 * state rather than call counts, since we use real implementations.</p>
 *
 * <p><b>Validates: Requirements 4.3, 4.4</b></p>
 */
class CircuitBreakerEventRecordingPropertyTest {

    @Property(tries = 50)
    void consecutiveSuccessesKeepCircuitClosed(
            @ForAll("thresholds") int threshold,
            @ForAll("successCounts") int successCount) {

        IndexEvalCircuitBreaker breaker = new IndexEvalCircuitBreaker(threshold, Duration.ofSeconds(60));
        String indexId = "test-index";

        for (int i = 0; i < successCount; i++) {
            breaker.recordSuccess(indexId);
        }

        assertThat(breaker.isAllowed(indexId))
                .as("Circuit must remain CLOSED after %d consecutive successes", successCount)
                .isTrue();
    }

    @Property(tries = 50)
    void singleSuccessAfterFailuresResetsState(
            @ForAll("thresholds") int threshold,
            @ForAll("failureCounts") int failuresBelowThreshold) {

        int failures = Math.min(failuresBelowThreshold, threshold - 1);
        IndexEvalCircuitBreaker breaker = new IndexEvalCircuitBreaker(threshold, Duration.ofSeconds(60));
        String indexId = "test-index";

        for (int i = 0; i < failures; i++) {
            breaker.recordFailure(indexId);
        }
        breaker.recordSuccess(indexId);

        assertThat(breaker.isAllowed(indexId))
                .as("Circuit must be CLOSED after success resets %d failures", failures)
                .isTrue();

        // After reset, need full threshold failures to re-open
        for (int i = 0; i < threshold - 1; i++) {
            breaker.recordFailure(indexId);
            assertThat(breaker.isAllowed(indexId)).isTrue();
        }
        breaker.recordFailure(indexId);
        assertThat(breaker.isAllowed(indexId))
                .as("Should need full threshold after reset")
                .isFalse();
    }

    @Property(tries = 50)
    void failureRecordingDrivesTowardOpen(
            @ForAll("thresholds") int threshold) {

        IndexEvalCircuitBreaker breaker = new IndexEvalCircuitBreaker(threshold, Duration.ofSeconds(60));
        String indexId = "test-index";

        for (int i = 0; i < threshold; i++) {
            breaker.recordFailure(indexId);
        }

        assertThat(breaker.isAllowed(indexId))
                .as("Exactly %d failures must open the circuit", threshold)
                .isFalse();
    }

    @Provide
    Arbitrary<Integer> thresholds() {
        return Arbitraries.integers().between(1, 10);
    }

    @Provide
    Arbitrary<Integer> successCounts() {
        return Arbitraries.integers().between(1, 50);
    }

    @Provide
    Arbitrary<Integer> failureCounts() {
        return Arbitraries.integers().between(1, 20);
    }
}
