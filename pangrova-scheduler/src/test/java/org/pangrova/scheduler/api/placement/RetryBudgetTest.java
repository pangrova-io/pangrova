package org.pangrova.scheduler.api.placement;

import java.time.Duration;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Unit tests for {@link RetryBudget} — builder validation, defaults, and backoff computation.
 */
class RetryBudgetTest {

    @Test
    void defaultBudgetHasSensibleValues() {
        RetryBudget budget = RetryBudget.withDefaults();

        assertThat(budget.maxRetriesPerTask()).isEqualTo(4);
        assertThat(budget.maxRetriesPerStage()).isEqualTo(8);
        assertThat(budget.maxRetriesPerQuery()).isEqualTo(16);
        assertThat(budget.initialBackoff()).isEqualTo(Duration.ofSeconds(10));
        assertThat(budget.maxBackoff()).isEqualTo(Duration.ofMinutes(1));
        assertThat(budget.backoffMultiplier()).isEqualTo(2.0);
    }

    @Test
    void builderOverridesDefaults() {
        RetryBudget budget = RetryBudget.builder()
                .maxRetriesPerTask(2)
                .maxRetriesPerStage(5)
                .maxRetriesPerQuery(10)
                .initialBackoff(Duration.ofSeconds(5))
                .maxBackoff(Duration.ofSeconds(30))
                .backoffMultiplier(1.5)
                .build();

        assertThat(budget.maxRetriesPerTask()).isEqualTo(2);
        assertThat(budget.maxRetriesPerStage()).isEqualTo(5);
        assertThat(budget.maxRetriesPerQuery()).isEqualTo(10);
        assertThat(budget.initialBackoff()).isEqualTo(Duration.ofSeconds(5));
        assertThat(budget.maxBackoff()).isEqualTo(Duration.ofSeconds(30));
        assertThat(budget.backoffMultiplier()).isEqualTo(1.5);
    }

    @Test
    void zeroRetriesAreAllowed() {
        RetryBudget budget = RetryBudget.builder()
                .maxRetriesPerTask(0)
                .maxRetriesPerStage(0)
                .maxRetriesPerQuery(0)
                .build();

        assertThat(budget.maxRetriesPerTask()).isZero();
        assertThat(budget.maxRetriesPerStage()).isZero();
        assertThat(budget.maxRetriesPerQuery()).isZero();
    }

    @Test
    void negativeMaxRetriesPerTaskThrows() {
        assertThatThrownBy(() -> RetryBudget.builder().maxRetriesPerTask(-1))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("maxRetriesPerTask");
    }

    @Test
    void negativeMaxRetriesPerStageThrows() {
        assertThatThrownBy(() -> RetryBudget.builder().maxRetriesPerStage(-1))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("maxRetriesPerStage");
    }

    @Test
    void negativeMaxRetriesPerQueryThrows() {
        assertThatThrownBy(() -> RetryBudget.builder().maxRetriesPerQuery(-1))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("maxRetriesPerQuery");
    }

    @Test
    void nullInitialBackoffThrows() {
        assertThatThrownBy(() -> RetryBudget.builder().initialBackoff(null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("initialBackoff");
    }

    @Test
    void negativeInitialBackoffThrows() {
        assertThatThrownBy(() -> RetryBudget.builder().initialBackoff(Duration.ofSeconds(-1)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("initialBackoff");
    }

    @Test
    void nullMaxBackoffThrows() {
        assertThatThrownBy(() -> RetryBudget.builder().maxBackoff(null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("maxBackoff");
    }

    @Test
    void backoffMultiplierBelowOneThrows() {
        assertThatThrownBy(() -> RetryBudget.builder().backoffMultiplier(0.5))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("backoffMultiplier");
    }

    @Test
    void maxBackoffLessThanInitialThrows() {
        assertThatThrownBy(() -> RetryBudget.builder()
                .initialBackoff(Duration.ofSeconds(30))
                .maxBackoff(Duration.ofSeconds(10))
                .build())
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("maxBackoff")
                .hasMessageContaining("initialBackoff");
    }

    @Test
    void computeBackoff_firstAttemptReturnsInitial() {
        RetryBudget budget = RetryBudget.builder()
                .initialBackoff(Duration.ofSeconds(5))
                .maxBackoff(Duration.ofMinutes(2))
                .backoffMultiplier(2.0)
                .build();

        assertThat(budget.computeBackoff(1)).isEqualTo(Duration.ofSeconds(5));
    }

    @Test
    void computeBackoff_exponentialGrowth() {
        RetryBudget budget = RetryBudget.builder()
                .initialBackoff(Duration.ofSeconds(1))
                .maxBackoff(Duration.ofMinutes(10))
                .backoffMultiplier(2.0)
                .build();

        assertThat(budget.computeBackoff(1)).isEqualTo(Duration.ofSeconds(1));
        assertThat(budget.computeBackoff(2)).isEqualTo(Duration.ofSeconds(2));
        assertThat(budget.computeBackoff(3)).isEqualTo(Duration.ofSeconds(4));
        assertThat(budget.computeBackoff(4)).isEqualTo(Duration.ofSeconds(8));
    }

    @Test
    void computeBackoff_cappedAtMaxBackoff() {
        RetryBudget budget = RetryBudget.builder()
                .initialBackoff(Duration.ofSeconds(10))
                .maxBackoff(Duration.ofSeconds(30))
                .backoffMultiplier(2.0)
                .build();

        // attempt 1: 10s, attempt 2: 20s, attempt 3: 40s -> capped at 30s
        assertThat(budget.computeBackoff(3)).isEqualTo(Duration.ofSeconds(30));
        assertThat(budget.computeBackoff(10)).isEqualTo(Duration.ofSeconds(30));
    }

    @Test
    void toStringContainsAllFields() {
        RetryBudget budget = RetryBudget.withDefaults();
        String str = budget.toString();

        assertThat(str)
                .contains("maxRetriesPerTask=4")
                .contains("maxRetriesPerStage=8")
                .contains("maxRetriesPerQuery=16")
                .contains("backoffMultiplier=2.0");
    }
}
