package org.pangrova.operations.scaling;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.concurrent.atomic.AtomicInteger;

import org.pangrova.operations.scaling.ShuffleResidencyCheck.ScaleDownAdvice;
import org.junit.jupiter.api.Test;

class AutoScalingControllerTest {

    /** A hand-advanced clock for deterministically driving cooldown windows. */
    private static final class MutableClock extends Clock {
        private Instant now = Instant.parse("2026-06-01T00:00:00Z");

        @Override public ZoneId getZone() {
            return ZoneOffset.UTC;
        }

        @Override public Clock withZone(ZoneId zone) {
            return this;
        }

        @Override public Instant instant() {
            return now;
        }

        void advance(Duration d) {
            now = now.plus(d);
        }
    }

    // Emergency threshold: CPU > 0.95 or Memory > 0.95 or Queue > 0.95 or Latency > 0.95
    private static final EmergencyThreshold EMERGENCY =
            new EmergencyThreshold(0.95, 0.95, 0.95, 0.95);

    // A snapshot that breaches emergency (CPU = 0.99)
    private static final MetricSnapshot EMERGENCY_SNAPSHOT =
            new MetricSnapshot(0.99, 0.5, 0.5, 0.5);

    // A normal high-load snapshot (triggers score-based scale-out but not emergency)
    private static final MetricSnapshot HIGH_LOAD =
            new MetricSnapshot(0.9, 0.8, 0.9, 0.8);

    // A low-load snapshot (no scaling action)
    private static final MetricSnapshot LOW_LOAD =
            new MetricSnapshot(0.3, 0.2, 0.2, 0.1);

    /**
     * Creates a controller with an AtomicInteger serving dual purpose: it is
     * both the scaling executor target (set on scale) and the authoritative
     * worker count supplier (read on evaluate). This models the real system
     * where the WorkerManager reflects scaling operations.
     */
    private AutoScalingController controller(MutableClock clock, AtomicInteger workerCount) {
        return new AutoScalingController(
                new ScalingPolicy(),
                EMERGENCY,
                workerCount::set,
                workerCount::get,
                clock);
    }

    /**
     * Creates a controller with a specific shuffle drain policy.
     */
    private AutoScalingController controllerWithShuffleDrain(
            MutableClock clock,
            AtomicInteger workerCount,
            ShuffleDrainPolicy drainPolicy) {
        return new AutoScalingController(
                new ScalingPolicy(),
                EMERGENCY,
                workerCount::set,
                workerCount::get,
                drainPolicy,
                clock);
    }

    // ── Emergency scale-out ──

    @Test
    void emergencyScaleOutBypassesNormalCooldown() {
        MutableClock clock = new MutableClock();
        AtomicInteger workerCount = new AtomicInteger(4);
        AutoScalingController c = controller(clock, workerCount);

        // First emergency scale-out succeeds (no prior cooldown)
        int result = c.evaluate(EMERGENCY_SNAPSHOT);
        assertThat(result).isEqualTo(6); // 4 + 2
        assertThat(workerCount.get()).isEqualTo(6);

        // Advance 1 minute — well within the normal 3-minute cooldown,
        // but past the 30-second emergency cooldown
        clock.advance(Duration.ofMinutes(1));

        // Second emergency scale-out should succeed (bypasses normal cooldown)
        // workerCount is now 6 (set by executor), so supplier returns 6
        result = c.evaluate(EMERGENCY_SNAPSHOT);
        assertThat(result).isEqualTo(8); // 6 + 2
        assertThat(workerCount.get()).isEqualTo(8);
    }

    @Test
    void emergencyScaleOutRespectsEmergencyCooldown() {
        MutableClock clock = new MutableClock();
        AtomicInteger workerCount = new AtomicInteger(4);
        AutoScalingController c = controller(clock, workerCount);

        // First emergency scale-out succeeds
        int result = c.evaluate(EMERGENCY_SNAPSHOT);
        assertThat(result).isEqualTo(6);

        // Advance only 10 seconds — within the 30-second emergency cooldown
        clock.advance(Duration.ofSeconds(10));

        // Second emergency scale-out should be blocked by emergency cooldown
        result = c.evaluate(EMERGENCY_SNAPSHOT);
        assertThat(result).isEqualTo(6); // no change, cooldown active
    }

    // ── Normal scale-out ──

    @Test
    void normalScaleOutStillRespectsNormalCooldown() {
        MutableClock clock = new MutableClock();
        AtomicInteger workerCount = new AtomicInteger(4);
        AutoScalingController c = controller(clock, workerCount);

        // First normal scale-out succeeds (HIGH_LOAD triggers score-based scale-out)
        int result = c.evaluate(HIGH_LOAD);
        assertThat(result).isGreaterThan(4);

        // Advance 1 minute — within normal 3-minute cooldown
        clock.advance(Duration.ofMinutes(1));

        // Second normal scale-out should be blocked by normal cooldown
        int afterCooldown = c.evaluate(HIGH_LOAD);
        assertThat(afterCooldown).isEqualTo(result); // no change
    }

    @Test
    void normalScaleOutSucceedsAfterNormalCooldownExpires() {
        MutableClock clock = new MutableClock();
        AtomicInteger workerCount = new AtomicInteger(4);
        AutoScalingController c = controller(clock, workerCount);

        // First normal scale-out
        int result = c.evaluate(HIGH_LOAD);
        assertThat(result).isGreaterThan(4);
        int firstTarget = result;

        // Advance past the normal 3-minute cooldown
        clock.advance(Duration.ofMinutes(4));

        // Now normal scale-out should work again
        int secondResult = c.evaluate(HIGH_LOAD);
        assertThat(secondResult).isGreaterThan(firstTarget);
    }

    @Test
    void emergencyAfterRecentNormalScaleOutStillSucceeds() {
        MutableClock clock = new MutableClock();
        AtomicInteger workerCount = new AtomicInteger(4);
        AutoScalingController c = controller(clock, workerCount);

        // Normal scale-out first
        int result = c.evaluate(HIGH_LOAD);
        assertThat(result).isGreaterThan(4);

        // Advance only 45 seconds — within normal cooldown but past emergency cooldown
        clock.advance(Duration.ofSeconds(45));

        // Emergency should still work despite recent normal scale-out
        // workerCount was set by the executor to `result`
        int emergencyResult = c.evaluate(EMERGENCY_SNAPSHOT);
        assertThat(emergencyResult).isEqualTo(result + 2);
    }

    // ── No scaling ──

    @Test
    void noScalingWhenLoadIsLow() {
        MutableClock clock = new MutableClock();
        AtomicInteger workerCount = new AtomicInteger(4);
        AutoScalingController c = controller(clock, workerCount);

        int result = c.evaluate(LOW_LOAD);
        assertThat(result).isEqualTo(4); // no change for low load at 4 workers
    }

    // ── Worker count supplier ──

    @Test
    void evaluateReadsFreshWorkerCountFromSupplier() {
        MutableClock clock = new MutableClock();
        // Simulate a supplier that returns a dynamic count independent of the executor.
        // This models the case where WorkerManager has an authoritative view.
        AtomicInteger authoritativeCount = new AtomicInteger(4);
        AtomicInteger executorTarget = new AtomicInteger(0);

        AutoScalingController c = new AutoScalingController(
                new ScalingPolicy(),
                EMERGENCY,
                executorTarget::set,
                authoritativeCount::get,
                clock);

        // First evaluate: supplier says 4 workers, emergency triggers scale to 6
        int result = c.evaluate(EMERGENCY_SNAPSHOT);
        assertThat(result).isEqualTo(6);
        assertThat(executorTarget.get()).isEqualTo(6);

        // Advance past emergency cooldown
        clock.advance(Duration.ofMinutes(1));

        // Simulate: the authoritative registry already shows 10 workers
        // (e.g., another controller scaled, or manual intervention)
        authoritativeCount.set(10);

        // Evaluate again — should read 10 from supplier, scale to 12 (emergency +2)
        result = c.evaluate(EMERGENCY_SNAPSHOT);
        assertThat(result).isEqualTo(12); // 10 + 2, NOT 6 + 2
        assertThat(executorTarget.get()).isEqualTo(12);
    }

    @Test
    void staleCallerViewDoesNotAffectScalingDecision() {
        MutableClock clock = new MutableClock();
        // Supplier represents live WorkerManager state
        AtomicInteger liveWorkerCount = new AtomicInteger(8);
        AtomicInteger executorTarget = new AtomicInteger(0);

        AutoScalingController c = new AutoScalingController(
                new ScalingPolicy(),
                EMERGENCY,
                executorTarget::set,
                liveWorkerCount::get,
                clock);

        // Use a truly idle snapshot (all zeros → score=0.0 < 0.2 threshold) to trigger scale-in.
        // The controller reads 8 from the supplier and scales in to 7.
        MetricSnapshot idle = new MetricSnapshot(0.0, 0.0, 0.0, 0.0);
        int result = c.evaluate(idle);
        assertThat(result).isEqualTo(7); // scale-in from 8 to 7
        assertThat(executorTarget.get()).isEqualTo(7);
    }

    // ── G-RES-08: Shuffle-aware scale-down ──

    @Test
    void scaleInBlockedWhenShuffleDrainFails() {
        MutableClock clock = new MutableClock();
        AtomicInteger workerCount = new AtomicInteger(8);

        // Residency check says drain is needed; drain executor fails
        ShuffleResidencyCheck check = (current, target) ->
                ScaleDownAdvice.allowWithDrain(target);
        ShuffleDrainPolicy drainPolicy = new ShuffleDrainPolicy(
                check,
                timeout -> false,  // drain fails
                Duration.ofSeconds(30));

        AutoScalingController c = controllerWithShuffleDrain(clock, workerCount, drainPolicy);

        // Idle snapshot triggers scale-in attempt
        MetricSnapshot idle = new MetricSnapshot(0.0, 0.0, 0.0, 0.0);
        int result = c.evaluate(idle);

        // Scale-in should be blocked because drain failed
        assertThat(result).isEqualTo(8);
        assertThat(workerCount.get()).isEqualTo(8);
    }

    @Test
    void scaleInProceedsAfterSuccessfulDrain() {
        MutableClock clock = new MutableClock();
        AtomicInteger workerCount = new AtomicInteger(8);

        // Residency check says drain is needed; drain succeeds
        ShuffleResidencyCheck check = (current, target) ->
                ScaleDownAdvice.allowWithDrain(target);
        ShuffleDrainPolicy drainPolicy = new ShuffleDrainPolicy(
                check,
                timeout -> true,  // drain succeeds
                Duration.ofSeconds(30));

        AutoScalingController c = controllerWithShuffleDrain(clock, workerCount, drainPolicy);

        MetricSnapshot idle = new MetricSnapshot(0.0, 0.0, 0.0, 0.0);
        int result = c.evaluate(idle);

        // Scale-in proceeds because drain succeeded
        assertThat(result).isEqualTo(7);
        assertThat(workerCount.get()).isEqualTo(7);
    }

    @Test
    void scaleInTargetAdjustedByResidencyCheck() {
        MutableClock clock = new MutableClock();
        AtomicInteger workerCount = new AtomicInteger(8);

        // Residency check adjusts target up (some workers have live shuffle data)
        ShuffleResidencyCheck check = (current, target) ->
                ScaleDownAdvice.adjust(current - 1,
                        "only 1 worker is safe to remove due to shuffle data");
        ShuffleDrainPolicy drainPolicy = new ShuffleDrainPolicy(
                check,
                timeout -> true,
                Duration.ofSeconds(30));

        AutoScalingController c = controllerWithShuffleDrain(clock, workerCount, drainPolicy);

        // Idle: policy wants to scale in by 1 (8->7), residency agrees (current-1=7)
        MetricSnapshot idle = new MetricSnapshot(0.0, 0.0, 0.0, 0.0);
        int result = c.evaluate(idle);

        assertThat(result).isEqualTo(7);
        assertThat(workerCount.get()).isEqualTo(7);
    }

    @Test
    void scaleInWithClearResidencyProceedsNormally() {
        MutableClock clock = new MutableClock();
        AtomicInteger workerCount = new AtomicInteger(8);

        // All workers clear — no shuffle data concerns
        ShuffleDrainPolicy drainPolicy = ShuffleDrainPolicy.noOp();

        AutoScalingController c = controllerWithShuffleDrain(clock, workerCount, drainPolicy);

        MetricSnapshot idle = new MetricSnapshot(0.0, 0.0, 0.0, 0.0);
        int result = c.evaluate(idle);

        assertThat(result).isEqualTo(7);
        assertThat(workerCount.get()).isEqualTo(7);
    }

    @Test
    void scaleInBlockedWhenResidencyAdjustsToCurrentCount() {
        MutableClock clock = new MutableClock();
        AtomicInteger workerCount = new AtomicInteger(8);

        // Residency check says no workers can be safely removed
        ShuffleResidencyCheck check = (current, target) ->
                ScaleDownAdvice.adjust(current,
                        "all workers have live ephemeral shuffle data");
        ShuffleDrainPolicy drainPolicy = new ShuffleDrainPolicy(
                check,
                timeout -> true,
                Duration.ofSeconds(30));

        AutoScalingController c = controllerWithShuffleDrain(clock, workerCount, drainPolicy);

        MetricSnapshot idle = new MetricSnapshot(0.0, 0.0, 0.0, 0.0);
        int result = c.evaluate(idle);

        // Cannot scale down at all — adjusted target equals current
        assertThat(result).isEqualTo(8);
        assertThat(workerCount.get()).isEqualTo(8);
    }

    @Test
    void scaleOutIsNotAffectedByShuffleDrainPolicy() {
        MutableClock clock = new MutableClock();
        AtomicInteger workerCount = new AtomicInteger(4);

        // Even a blocking drain policy should not affect scale-out
        ShuffleResidencyCheck check = (current, target) ->
                ScaleDownAdvice.adjust(current, "always block");
        ShuffleDrainPolicy drainPolicy = new ShuffleDrainPolicy(
                check,
                timeout -> false,
                Duration.ofSeconds(30));

        AutoScalingController c = controllerWithShuffleDrain(clock, workerCount, drainPolicy);

        // Scale-out should work regardless of drain policy
        int result = c.evaluate(EMERGENCY_SNAPSHOT);
        assertThat(result).isEqualTo(6); // emergency: 4 + 2
        assertThat(workerCount.get()).isEqualTo(6);
    }
}
