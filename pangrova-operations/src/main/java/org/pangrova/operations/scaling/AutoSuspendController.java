package org.pangrova.operations.scaling;

import org.pangrova.common.annotation.ExperimentalImplementation;
import org.pangrova.common.concurrency.ThreadSafe;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.Objects;

/**
 * Idle-based scale-in / auto-suspend decision function: recommends scaling the cluster down to
 * an idle floor once it has been continuously idle for a configured duration. Complements
 * {@link AutoScalingController} (which handles load-driven scale-out and score-based scale-in)
 * with the missing "wind down an idle cluster" lever — the basis of serverless auto-suspend
 * (cf. Snowflake warehouse auto-suspend, Databricks SQL warehouse idle scale-down).
 *
 * <p>A snapshot counts as idle when its composite {@link ScalingPolicy} score is at or below
 * {@code idleScoreThreshold} <em>and</em> there is no queued work. Transient activity resets the
 * idle timer, so only <em>sustained</em> idleness suspends — avoiding flapping on brief lulls.</p>
 *
 * <p>Concurrency: {@code idleSince} is a {@code volatile} best-effort timer, mirroring
 * {@link AutoScalingController}'s cooldown approach; the scaling executor is idempotent.</p>
 */
@ExperimentalImplementation(trackingIssue = "https://github.com/pangrova/pangrova/issues/F-OPS-002-auto-scaling")
@ThreadSafe
public final class AutoSuspendController {

    private final ScalingPolicy policy;
    private final double idleScoreThreshold;
    private final Duration idleDuration;
    private final int minWorkers;
    private final Clock clock;

    private volatile Instant idleSince;

    public AutoSuspendController(ScalingPolicy policy, double idleScoreThreshold,
                                Duration idleDuration, int minWorkers) {
        this(policy, idleScoreThreshold, idleDuration, minWorkers, Clock.systemUTC());
    }

    /** Full constructor with an explicit clock for idle tracking. */
    public AutoSuspendController(ScalingPolicy policy, double idleScoreThreshold,
                                Duration idleDuration, int minWorkers, Clock clock) {
        this.policy = Objects.requireNonNull(policy, "policy");
        if (idleScoreThreshold < 0.0 || idleScoreThreshold > 1.0) {
            throw new IllegalArgumentException(
                    "idleScoreThreshold must be in [0.0, 1.0], got " + idleScoreThreshold);
        }
        this.idleDuration = Objects.requireNonNull(idleDuration, "idleDuration");
        if (idleDuration.isNegative() || idleDuration.isZero()) {
            throw new IllegalArgumentException("idleDuration must be positive, got " + idleDuration);
        }
        if (minWorkers < 0) {
            throw new IllegalArgumentException("minWorkers must be >= 0, got " + minWorkers);
        }
        this.idleScoreThreshold = idleScoreThreshold;
        this.minWorkers = minWorkers;
        this.clock = Objects.requireNonNull(clock, "clock");
    }

    /**
     * Evaluates whether sustained idleness warrants suspending the cluster.
     *
     * @param snapshot       current cluster metrics
     * @param currentWorkers current worker count
     * @return the suspend decision (hold, or suspend down to the idle floor)
     */
    public SuspendDecision evaluate(MetricSnapshot snapshot, int currentWorkers) {
        Objects.requireNonNull(snapshot, "snapshot");
        // Already at or below the floor: nothing to wind down; reset the timer.
        if (currentWorkers <= minWorkers) {
            idleSince = null;
            return SuspendDecision.hold(currentWorkers, Duration.ZERO);
        }
        if (!isIdle(snapshot)) {
            idleSince = null;
            return SuspendDecision.hold(currentWorkers, Duration.ZERO);
        }
        Instant now = clock.instant();
        Instant since = idleSince;
        if (since == null) {
            idleSince = now;
            return SuspendDecision.hold(currentWorkers, Duration.ZERO);
        }
        Duration idleFor = Duration.between(since, now);
        if (idleFor.compareTo(idleDuration) >= 0) {
            return SuspendDecision.suspend(minWorkers, idleFor);
        }
        return SuspendDecision.hold(currentWorkers, idleFor);
    }

    private boolean isIdle(MetricSnapshot snapshot) {
        return snapshot.queueDepthNormalized() <= 0.0
                && policy.calculateScore(snapshot) <= idleScoreThreshold;
    }
}
