package org.pangrova.operations.scaling;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import org.junit.jupiter.api.Test;

class AutoSuspendControllerTest {

    /** A hand-advanced clock for deterministically driving the idle timer. */
    private static final class MutableClock extends Clock {
        private Instant now = Instant.parse("2026-05-31T00:00:00Z");

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

    private static final MetricSnapshot IDLE = new MetricSnapshot(0.01, 0.01, 0.0, 0.01);
    private static final MetricSnapshot BUSY = new MetricSnapshot(0.9, 0.5, 0.8, 0.4);
    private static final Duration IDLE_WINDOW = Duration.ofMinutes(15);

    private AutoSuspendController controller(MutableClock clock) {
        return new AutoSuspendController(new ScalingPolicy(), 0.05, IDLE_WINDOW, 0, clock);
    }

    @Test
    void sustainedIdleTriggersSuspend() {
        MutableClock clock = new MutableClock();
        AutoSuspendController c = controller(clock);
        // First idle observation only arms the timer.
        assertThat(c.evaluate(IDLE, 4).suspend()).isFalse();
        // Not yet past the window.
        clock.advance(Duration.ofMinutes(14));
        assertThat(c.evaluate(IDLE, 4).suspend()).isFalse();
        // Past the window → suspend down to floor (0).
        clock.advance(Duration.ofMinutes(1));
        SuspendDecision d = c.evaluate(IDLE, 4);
        assertThat(d.suspend()).isTrue();
        assertThat(d.targetWorkers()).isZero();
        assertThat(d.idleFor()).isGreaterThanOrEqualTo(IDLE_WINDOW);
    }

    @Test
    void activityResetsTheIdleTimer() {
        MutableClock clock = new MutableClock();
        AutoSuspendController c = controller(clock);
        c.evaluate(IDLE, 4);             // arm
        clock.advance(Duration.ofMinutes(14));
        c.evaluate(BUSY, 4);             // activity resets
        clock.advance(Duration.ofMinutes(14));
        // Only 14 min idle again since reset → no suspend.
        assertThat(c.evaluate(IDLE, 4).suspend()).isFalse();
    }

    @Test
    void queuedWorkIsNotIdleEvenWhenUtilizationLow() {
        MutableClock clock = new MutableClock();
        AutoSuspendController c = controller(clock);
        MetricSnapshot lowUtilButQueued = new MetricSnapshot(0.01, 0.01, 0.3, 0.01);
        c.evaluate(lowUtilButQueued, 4);
        clock.advance(IDLE_WINDOW.plusMinutes(5));
        assertThat(c.evaluate(lowUtilButQueued, 4).suspend()).isFalse();
    }

    @Test
    void alreadyAtFloorHolds() {
        MutableClock clock = new MutableClock();
        AutoSuspendController c = controller(clock);
        clock.advance(IDLE_WINDOW.plusMinutes(5));
        SuspendDecision d = c.evaluate(IDLE, 0);
        assertThat(d.suspend()).isFalse();
        assertThat(d.targetWorkers()).isZero();
    }

    @Test
    void respectsNonZeroFloor() {
        MutableClock clock = new MutableClock();
        AutoSuspendController c =
                new AutoSuspendController(new ScalingPolicy(), 0.05, IDLE_WINDOW, 1, clock);
        c.evaluate(IDLE, 3);
        clock.advance(IDLE_WINDOW.plusMinutes(1));
        SuspendDecision d = c.evaluate(IDLE, 3);
        assertThat(d.suspend()).isTrue();
        assertThat(d.targetWorkers()).isEqualTo(1);
        // At the floor, further evaluation holds.
        assertThat(c.evaluate(IDLE, 1).suspend()).isFalse();
    }

    @Test
    void rejectsInvalidConfiguration() {
        assertThatThrownBy(() -> new AutoSuspendController(new ScalingPolicy(), 1.5, IDLE_WINDOW, 0))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new AutoSuspendController(new ScalingPolicy(), 0.05, Duration.ZERO, 0))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new AutoSuspendController(new ScalingPolicy(), 0.05, IDLE_WINDOW, -1))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
