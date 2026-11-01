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
 * Property 5: Circuit breaker OPEN state blocks evaluation.
 *
 * <p>For any index evaluation request, when IndexEvalCircuitBreaker is OPEN,
 * isAllowed returns false and no actual evaluation logic executes.</p>
 *
 * <p><b>Validates: Requirement 4.2</b></p>
 */
class CircuitBreakerOpenBlockingPropertyTest {

    @Property(tries = 50)
    void openCircuitBlocksAllRequests(
            @ForAll("thresholds") int threshold,
            @ForAll("indexIds") String indexId) {

        IndexEvalCircuitBreaker breaker = new IndexEvalCircuitBreaker(threshold, Duration.ofSeconds(300));

        // Drive to OPEN state
        for (int i = 0; i < threshold; i++) {
            breaker.recordFailure(indexId);
        }

        // Verify all subsequent requests are blocked
        for (int i = 0; i < 10; i++) {
            assertThat(breaker.isAllowed(indexId))
                    .as("OPEN circuit must block request %d for indexId=%s", i, indexId)
                    .isFalse();
        }
    }

    @Property(tries = 50)
    void openCircuitDoesNotAffectOtherIndexes(
            @ForAll("thresholds") int threshold) {

        IndexEvalCircuitBreaker breaker = new IndexEvalCircuitBreaker(threshold, Duration.ofSeconds(300));
        String failingIndex = "failing-index";
        String healthyIndex = "healthy-index";

        // Open circuit for failingIndex only
        for (int i = 0; i < threshold; i++) {
            breaker.recordFailure(failingIndex);
        }

        assertThat(breaker.isAllowed(failingIndex)).isFalse();
        assertThat(breaker.isAllowed(healthyIndex))
                .as("Healthy index must not be affected by another index's OPEN circuit")
                .isTrue();
    }

    @Provide
    Arbitrary<Integer> thresholds() {
        return Arbitraries.integers().between(1, 10);
    }

    @Provide
    Arbitrary<String> indexIds() {
        return Arbitraries.strings().alpha().ofMinLength(1).ofMaxLength(20);
    }
}
