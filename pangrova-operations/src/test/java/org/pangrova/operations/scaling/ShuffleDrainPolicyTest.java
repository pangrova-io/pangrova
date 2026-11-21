package org.pangrova.operations.scaling;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicBoolean;

import org.pangrova.operations.scaling.ShuffleResidencyCheck.ScaleDownAdvice;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link ShuffleDrainPolicy} and its integration with
 * {@link ShuffleDataResidencyCheck} (G-RES-08).
 */
class ShuffleDrainPolicyTest {

    // ── ScaleDownResult ──

    @Test
    void readyResultIsReady() {
        ShuffleDrainPolicy.ScaleDownResult result =
                ShuffleDrainPolicy.ScaleDownResult.ready(5, "all clear");
        assertThat(result.ready()).isTrue();
        assertThat(result.adjustedTarget()).isEqualTo(5);
        assertThat(result.reason()).isEqualTo("all clear");
    }

    @Test
    void blockedResultIsNotReady() {
        ShuffleDrainPolicy.ScaleDownResult result =
                ShuffleDrainPolicy.ScaleDownResult.blocked(8, "drain failed");
        assertThat(result.ready()).isFalse();
        assertThat(result.adjustedTarget()).isEqualTo(8);
        assertThat(result.reason()).isEqualTo("drain failed");
    }

    @Test
    void negativeAdjustedTargetThrows() {
        assertThatThrownBy(() ->
                new ShuffleDrainPolicy.ScaleDownResult(-1, true, "bad"))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void nullReasonThrows() {
        assertThatThrownBy(() ->
                new ShuffleDrainPolicy.ScaleDownResult(5, true, null))
                .isInstanceOf(NullPointerException.class);
    }

    @Test
    void unknownResidencyPreservesTheCurrentPool() {
        var result = ShuffleDrainPolicy.preserveWorkers().prepareScaleDown(8, 6);
        assertThat(result.adjustedTarget()).isEqualTo(8);
        assertThat(result.reason()).contains("unknown");
    }

    // ── Clear residency (no drain needed) ──

    @Test
    void clearResidencyProceedsWithoutDrain() {
        AtomicBoolean drainCalled = new AtomicBoolean(false);
        ShuffleDrainPolicy policy = new ShuffleDrainPolicy(
                (current, target) -> ShuffleResidencyCheck.ScaleDownAdvice.allow(target),
                timeout -> { drainCalled.set(true); return true; },
                Duration.ofSeconds(30));

        ShuffleDrainPolicy.ScaleDownResult result = policy.prepareScaleDown(8, 6);

        assertThat(result.ready()).isTrue();
        assertThat(result.adjustedTarget()).isEqualTo(6);
        assertThat(drainCalled.get()).isFalse();
    }

    // ── Drain needed and succeeds ──

    @Test
    void drainNeededAndSucceedsProceedsAfterDrain() {
        ShuffleResidencyCheck check = (current, target) ->
                ScaleDownAdvice.allowWithDrain(target);

        AtomicBoolean drainCalled = new AtomicBoolean(false);
        ShuffleDrainPolicy policy = new ShuffleDrainPolicy(
                check,
                timeout -> { drainCalled.set(true); return true; },
                Duration.ofSeconds(30));

        ShuffleDrainPolicy.ScaleDownResult result = policy.prepareScaleDown(8, 6);

        assertThat(result.ready()).isTrue();
        assertThat(result.adjustedTarget()).isEqualTo(6);
        assertThat(drainCalled.get()).isTrue();
    }

    // ── Drain needed but fails ──

    @Test
    void drainNeededButFailsBlocksScaleDown() {
        ShuffleResidencyCheck check = (current, target) ->
                ScaleDownAdvice.allowWithDrain(target);

        ShuffleDrainPolicy policy = new ShuffleDrainPolicy(
                check,
                timeout -> false,  // drain fails
                Duration.ofSeconds(30));

        ShuffleDrainPolicy.ScaleDownResult result = policy.prepareScaleDown(8, 6);

        assertThat(result.ready()).isFalse();
        assertThat(result.adjustedTarget()).isEqualTo(8);  // stays at current
        assertThat(result.reason()).contains("drain failed");
    }

    // ── Adjusted target (some workers blocked) ──

    @Test
    void adjustedTargetRespectsResidencyCheck() {
        ShuffleResidencyCheck check = (current, target) ->
                ScaleDownAdvice.adjust(target + 1,
                        "1 worker has live ephemeral shuffle data");

        ShuffleDrainPolicy policy = new ShuffleDrainPolicy(
                check,
                timeout -> true,
                Duration.ofSeconds(30));

        ShuffleDrainPolicy.ScaleDownResult result = policy.prepareScaleDown(8, 5);

        assertThat(result.ready()).isTrue();
        assertThat(result.adjustedTarget()).isEqualTo(6);  // 5 + 1
        assertThat(result.reason()).contains("1 worker has live ephemeral");
    }

    // ── Drain executor receives configured timeout ──

    @Test
    void drainExecutorReceivesConfiguredTimeout() {
        Duration configuredTimeout = Duration.ofMinutes(2);
        Duration[] receivedTimeout = new Duration[1];

        ShuffleResidencyCheck check = (current, target) ->
                ScaleDownAdvice.allowWithDrain(target);

        ShuffleDrainPolicy policy = new ShuffleDrainPolicy(
                check,
                timeout -> { receivedTimeout[0] = timeout; return true; },
                configuredTimeout);

        policy.prepareScaleDown(8, 6);

        assertThat(receivedTimeout[0]).isEqualTo(configuredTimeout);
    }

    // ── Constructor validation ──

    @Test
    void nullResidencyCheckThrows() {
        assertThatThrownBy(() ->
                new ShuffleDrainPolicy(null, timeout -> true, Duration.ofSeconds(30)))
                .isInstanceOf(NullPointerException.class);
    }

    @Test
    void nullDrainExecutorThrows() {
        assertThatThrownBy(() ->
                new ShuffleDrainPolicy((current, target) -> ShuffleResidencyCheck.ScaleDownAdvice.allow(target), null,
                        Duration.ofSeconds(30)))
                .isInstanceOf(NullPointerException.class);
    }

    @Test
    void zeroDrainTimeoutThrows() {
        assertThatThrownBy(() ->
                new ShuffleDrainPolicy((current, target) -> ShuffleResidencyCheck.ScaleDownAdvice.allow(target),
                        timeout -> true, Duration.ZERO))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void negativeDrainTimeoutThrows() {
        assertThatThrownBy(() ->
                new ShuffleDrainPolicy((current, target) -> ShuffleResidencyCheck.ScaleDownAdvice.allow(target),
                        timeout -> true, Duration.ofSeconds(-5)))
                .isInstanceOf(IllegalArgumentException.class);
    }

    // ── ScaleDownAdvice record ──

    @Test
    void scaleDownAdviceAllowFactory() {
        ScaleDownAdvice advice = ScaleDownAdvice.allow(6);
        assertThat(advice.safeTarget()).isEqualTo(6);
        assertThat(advice.drainNeeded()).isFalse();
    }

    @Test
    void scaleDownAdviceAllowWithDrainFactory() {
        ScaleDownAdvice advice = ScaleDownAdvice.allowWithDrain(6);
        assertThat(advice.safeTarget()).isEqualTo(6);
        assertThat(advice.drainNeeded()).isTrue();
    }

    @Test
    void scaleDownAdviceAdjustFactory() {
        ScaleDownAdvice advice = ScaleDownAdvice.adjust(7, "blocked worker");
        assertThat(advice.safeTarget()).isEqualTo(7);
        assertThat(advice.drainNeeded()).isFalse();
        assertThat(advice.reason()).isEqualTo("blocked worker");
    }

    @Test
    void scaleDownAdviceNegativeTargetThrows() {
        assertThatThrownBy(() -> new ScaleDownAdvice(-1, false, "bad"))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void scaleDownAdviceNullReasonThrows() {
        assertThatThrownBy(() -> new ScaleDownAdvice(5, false, null))
                .isInstanceOf(NullPointerException.class);
    }
}
