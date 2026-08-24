package org.pangrova.common.concurrent;

import org.pangrova.common.exception.PangrovaErrorCode;
import org.pangrova.common.exception.PangrovaException;
import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Unit tests for {@link RetryingCommitWrapper}.
 * Verifies exponential backoff retry behavior for OCC commit conflicts.
 */
class RetryingCommitWrapperTest {

    @Test
    void succeedsOnFirstAttempt() {
        var wrapper = RetryingCommitWrapper.withDefaults(e -> true);
        AtomicInteger counter = new AtomicInteger(0);

        wrapper.execute(counter::incrementAndGet);

        assertThat(counter.get()).isEqualTo(1);
    }

    @Test
    void retriesOnConflictAndSucceedsOnThirdAttempt() {
        var wrapper = RetryingCommitWrapper.create(10, 100, 5,
                e -> e.getMessage() != null && e.getMessage().contains("conflict"));
        AtomicInteger attempts = new AtomicInteger(0);

        wrapper.execute(() -> {
            int attempt = attempts.incrementAndGet();
            if (attempt <= 2) {
                throw new RuntimeException("OCC conflict detected on attempt " + attempt);
            }
        });

        assertThat(attempts.get()).isEqualTo(3);
    }

    @Test
    void throwsAfterMaxRetriesExhausted() {
        var wrapper = RetryingCommitWrapper.create(10, 50, 2,
                e -> e instanceof IllegalStateException);
        AtomicInteger attempts = new AtomicInteger(0);

        assertThatThrownBy(() -> wrapper.execute(() -> {
            attempts.incrementAndGet();
            throw new IllegalStateException("always fails");
        }))
                .isInstanceOf(PangrovaException.class)
                .hasMessageContaining("OCC commit failed after 3 attempts")
                .satisfies(e -> assertThat(((PangrovaException) e).getErrorCode())
                        .isEqualTo(PangrovaErrorCode.RESILIENCE_RETRY_EXHAUSTED));

        assertThat(attempts.get()).isEqualTo(3);
    }

    @Test
    void nonRetryableExceptionPropagatesImmediately() {
        var wrapper = RetryingCommitWrapper.withDefaults(
                e -> e instanceof RuntimeException && e.getMessage().contains("conflict"));
        AtomicInteger attempts = new AtomicInteger(0);

        assertThatThrownBy(() -> wrapper.execute(() -> {
            attempts.incrementAndGet();
            throw new IllegalArgumentException("schema mismatch");
        }))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("schema mismatch");

        assertThat(attempts.get()).isEqualTo(1);
    }

    @Test
    void jitterProducesValueWithinExpectedRange() {
        for (int i = 0; i < 100; i++) {
            long jittered = RetryingCommitWrapper.applyJitter(1000);
            assertThat(jittered).isBetween(500L, 1000L);
        }
    }

    @Test
    void createRejectsInvalidParameters() {
        assertThatThrownBy(() -> RetryingCommitWrapper.create(0, 100, 5, e -> true))
                .isInstanceOf(IllegalArgumentException.class);

        assertThatThrownBy(() -> RetryingCommitWrapper.create(200, 100, 5, e -> true))
                .isInstanceOf(IllegalArgumentException.class);

        assertThatThrownBy(() -> RetryingCommitWrapper.create(100, 5000, -1, e -> true))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
