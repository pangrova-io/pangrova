package org.pangrova.common.client.session;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.HashSet;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Unit tests for {@link RetryPolicy}.
 * Covers delay computation, jitter, and validation.
 */
class RetryPolicyTest {

    @Test
    void rejectsInitialDelayThatCannotBeRepresentedByTheActualMillisecondSleeper() {
        Duration overflow = Duration.ofMillis(Long.MAX_VALUE).plusMillis(1);
        assertThatThrownBy(() -> new RetryPolicy(1, overflow, overflow, 1.0))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("initialDelay");
    }

    @Test
    void rejectsMaxDelayThatCannotBeRepresentedByTheActualMillisecondSleeper() {
        Duration overflow = Duration.ofMillis(Long.MAX_VALUE).plusMillis(1);
        assertThatThrownBy(() -> new RetryPolicy(1, Duration.ofMillis(1), overflow, 1.0))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("maxDelay");
    }

    @Test
    void jitterAcceptsTheLargestRepresentableMillisecondDelayWithoutOverflow() {
        var maximum = Duration.ofMillis(Long.MAX_VALUE);
        var policy = new RetryPolicy(1, maximum, maximum, 1.0);
        for (int sample = 0; sample < 100; sample++) {
            assertThat(policy.delayForAttemptWithJitter(0).toMillis()).isBetween(1L, Long.MAX_VALUE);
        }
    }

    @Test
    void rejectsNonFiniteBackoffMultipliers() {
        for (double multiplier : new double[] {Double.NaN, Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY}) {
            assertThatThrownBy(() -> new RetryPolicy(1, Duration.ofMillis(1), Duration.ofSeconds(1), multiplier))
                    .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("backoffMultiplier");
        }
    }

    @Test
    void initialDelayAlsoRespectsTheConfiguredCap() {
        var policy = new RetryPolicy(1, Duration.ofSeconds(2), Duration.ofSeconds(1), 2.0);
        assertThat(policy.delayForAttempt(0)).isEqualTo(Duration.ofSeconds(1));
    }

    @Test
    void zeroDelayDoesNotInventAnExtraMillisecond() {
        assertThat(RetryPolicy.NONE.delayForAttemptWithJitter(0)).isEqualTo(Duration.ZERO);
    }

    // ── delayForAttempt (deterministic) ──

    @Test
    void delayForAttempt_returnsExponentialSequence() {
        RetryPolicy policy = new RetryPolicy(5, Duration.ofSeconds(1), Duration.ofSeconds(30), 2.0);

        assertThat(policy.delayForAttempt(0)).isEqualTo(Duration.ofSeconds(1));
        assertThat(policy.delayForAttempt(1)).isEqualTo(Duration.ofSeconds(2));
        assertThat(policy.delayForAttempt(2)).isEqualTo(Duration.ofSeconds(4));
        assertThat(policy.delayForAttempt(3)).isEqualTo(Duration.ofSeconds(8));
        assertThat(policy.delayForAttempt(4)).isEqualTo(Duration.ofSeconds(16));
    }

    @Test
    void delayForAttempt_capsAtMaxDelay() {
        RetryPolicy policy = new RetryPolicy(10, Duration.ofSeconds(1), Duration.ofSeconds(10), 2.0);

        // 1 -> 2 -> 4 -> 8 -> 10 (capped)
        assertThat(policy.delayForAttempt(4)).isEqualTo(Duration.ofSeconds(10));
        assertThat(policy.delayForAttempt(5)).isEqualTo(Duration.ofSeconds(10));
    }

    // ── delayForAttemptWithJitter ──

    @Test
    void delayForAttemptWithJitter_returnsValueWithinExpectedRange() {
        RetryPolicy policy = new RetryPolicy(5, Duration.ofSeconds(1), Duration.ofSeconds(30), 2.0);

        // For attempt 0, base delay is 1000ms; jittered should be in [1, 1000]
        for (int i = 0; i < 100; i++) {
            Duration jittered = policy.delayForAttemptWithJitter(0);
            assertThat(jittered.toMillis()).isBetween(1L, 1000L);
        }
    }

    @Test
    void delayForAttemptWithJitter_respectsExponentialGrowth() {
        RetryPolicy policy = new RetryPolicy(5, Duration.ofSeconds(1), Duration.ofSeconds(30), 2.0);

        // For attempt 2, base delay is 4000ms; jittered should be in [1, 4000]
        for (int i = 0; i < 100; i++) {
            Duration jittered = policy.delayForAttemptWithJitter(2);
            assertThat(jittered.toMillis()).isBetween(1L, 4000L);
        }
    }

    @Test
    void delayForAttemptWithJitter_producesVaryingValues() {
        RetryPolicy policy = new RetryPolicy(5, Duration.ofSeconds(1), Duration.ofSeconds(30), 2.0);

        // Multiple calls should produce at least 2 distinct values
        Set<Long> values = new HashSet<>();
        for (int i = 0; i < 20; i++) {
            values.add(policy.delayForAttemptWithJitter(1).toMillis());
        }
        assertThat(values.size()).isGreaterThan(1);
    }

    @Test
    void delayForAttemptWithJitter_respectsMaxDelayCap() {
        RetryPolicy policy = new RetryPolicy(10, Duration.ofSeconds(1), Duration.ofSeconds(10), 2.0);

        // For high attempt number where cap is reached (base=10000ms)
        for (int i = 0; i < 100; i++) {
            Duration jittered = policy.delayForAttemptWithJitter(5);
            assertThat(jittered.toMillis()).isBetween(1L, 10_000L);
        }
    }

    // ── validation ──

    @Test
    void constructor_rejectsNegativeMaxRetries() {
        assertThatThrownBy(() -> new RetryPolicy(-1, Duration.ofSeconds(1), Duration.ofSeconds(30), 2.0))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("maxRetries");
    }

    @Test
    void constructor_rejectsNegativeInitialDelay() {
        assertThatThrownBy(() -> new RetryPolicy(3, Duration.ofMillis(-1), Duration.ofSeconds(30), 2.0))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void constructor_rejectsBackoffMultiplierBelowOne() {
        assertThatThrownBy(() -> new RetryPolicy(3, Duration.ofSeconds(1), Duration.ofSeconds(30), 0.5))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("backoffMultiplier");
    }

    // ── constants ──

    @Test
    void defaultPolicy_hasExpectedValues() {
        assertThat(RetryPolicy.DEFAULT.maxRetries()).isEqualTo(3);
        assertThat(RetryPolicy.DEFAULT.initialDelay()).isEqualTo(Duration.ofSeconds(1));
        assertThat(RetryPolicy.DEFAULT.maxDelay()).isEqualTo(Duration.ofSeconds(30));
        assertThat(RetryPolicy.DEFAULT.backoffMultiplier()).isEqualTo(2.0);
    }

    @Test
    void nonePolicy_hasZeroRetries() {
        assertThat(RetryPolicy.NONE.maxRetries()).isEqualTo(0);
    }
}
