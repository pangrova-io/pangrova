package org.pangrova.operations.scaling;

import org.pangrova.common.annotation.ExperimentalImplementation;
import org.pangrova.common.concurrency.ThreadSafe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.IntSupplier;

/**
 * Evaluates metrics and triggers scaling with cooldown enforcement and shuffle-aware
 * scale-down (G-RES-08).
 *
 * <p>Concurrency strategy: shared mutable state ({@code lastScaleOutTime}, {@code lastScaleInTime},
 * {@code lastEmergencyScaleOutTime}) is guarded by {@code volatile} reads and writes. The evaluate
 * method is safe to call from multiple threads; cooldown checks use volatile fields for visibility.
 * Concurrent callers may both pass the cooldown check and trigger scaling — this is by design, as
 * cooldown is best-effort, not transactional. The slight over-scaling risk is acceptable because
 * the scaling executor is idempotent (setting replicas to N twice is harmless).</p>
 *
 * <p>Scale-down is guarded by a {@link ShuffleDrainPolicy} that checks shuffle data residency
 * before removing workers (G-RES-08). If workers targeted for removal hold live shuffle data,
 * the policy triggers a drain window to flush or migrate data before proceeding. If drain fails,
 * scale-down is blocked to prevent shuffle data loss and upstream re-execution.</p>
 *
 * <p>Emergency scale-out has its own shorter cooldown (30 seconds) to allow rapid response
 * to resource pressure, independent of the normal scale-out cooldown (3 minutes).</p>
 */
@ExperimentalImplementation(trackingIssue = "https://github.com/pangrova/pangrova/issues/F-OPS-002-auto-scaling")
@ThreadSafe
public final class AutoScalingController {

    private static final Logger LOG = LoggerFactory.getLogger(AutoScalingController.class);
    private static final Duration SCALE_OUT_COOLDOWN = Duration.ofMinutes(3);
    private static final Duration SCALE_IN_COOLDOWN = Duration.ofMinutes(10);
    private static final Duration EMERGENCY_COOLDOWN = Duration.ofSeconds(30);

    private final ScalingPolicy policy;
    private final EmergencyThreshold emergencyThreshold;
    private final ScalingExecutor executor;
    private final IntSupplier workerCountSupplier;
    private final ShuffleDrainPolicy shuffleDrainPolicy;
    private final Clock clock;
    private final AtomicReference<Instant> lastScaleOutTime = new AtomicReference<>(Instant.EPOCH);
    private final AtomicReference<Instant> lastScaleInTime = new AtomicReference<>(Instant.EPOCH);
    private final AtomicReference<Instant> lastEmergencyScaleOutTime = new AtomicReference<>(Instant.EPOCH);

    /**
     * Creates a controller with shuffle drain policy, worker count supplier, and clock.
     *
     * @param policy              scoring policy for scaling decisions
     * @param emergencyThreshold  thresholds for emergency scale-out bypass
     * @param executor            executes scaling operations
     * @param workerCountSupplier supplies the current authoritative worker count
     * @param shuffleDrainPolicy  coordinates shuffle drain before scale-down (G-RES-08)
     * @param clock               clock for cooldown tracking (testable)
     */
    public AutoScalingController(
            ScalingPolicy policy,
            EmergencyThreshold emergencyThreshold,
            ScalingExecutor executor,
            IntSupplier workerCountSupplier,
            ShuffleDrainPolicy shuffleDrainPolicy,
            Clock clock) {
        this.policy = Objects.requireNonNull(policy, "policy");
        this.emergencyThreshold = Objects.requireNonNull(emergencyThreshold, "emergencyThreshold");
        this.executor = Objects.requireNonNull(executor, "executor");
        this.workerCountSupplier = Objects.requireNonNull(workerCountSupplier, "workerCountSupplier");
        this.shuffleDrainPolicy = Objects.requireNonNull(shuffleDrainPolicy, "shuffleDrainPolicy");
        this.clock = Objects.requireNonNull(clock, "clock");
    }

    /**
     * Creates a controller that only scales out until an authoritative shuffle drain policy is supplied.
     */
    public AutoScalingController(
            ScalingPolicy policy,
            EmergencyThreshold emergencyThreshold,
            ScalingExecutor executor,
            IntSupplier workerCountSupplier,
            Clock clock) {
        this(policy, emergencyThreshold, executor, workerCountSupplier,
                ShuffleDrainPolicy.preserveWorkers(), clock);
    }

    /**
     * Creates a scale-out-only controller using the system clock.
     */
    public AutoScalingController(
            ScalingPolicy policy,
            EmergencyThreshold emergencyThreshold,
            ScalingExecutor executor,
            IntSupplier workerCountSupplier) {
        this(policy, emergencyThreshold, executor, workerCountSupplier, Clock.systemUTC());
    }

    /**
     * Evaluates the current metric snapshot and decides whether to scale out, scale in, or hold.
     *
     * <p>The current worker count is read from the {@code workerCountSupplier} provided at
     * construction, ensuring the controller always acts on the authoritative count rather
     * than a stale caller-provided value.</p>
     *
     * @param snapshot current cluster metrics
     * @return the target worker count after evaluation
     */
    public int evaluate(MetricSnapshot snapshot) {
        int currentWorkers = workerCountSupplier.getAsInt();
        if (emergencyThreshold.isBreached(snapshot)) {
            return executeEmergencyScaleOut(currentWorkers);
        }
        double score = policy.calculateScore(snapshot);
        return evaluateByScore(score, currentWorkers);
    }

    private int evaluateByScore(double score, int currentWorkers) {
        int scaleOutTarget = policy.calculateScaleOutTarget(score, currentWorkers);
        if (scaleOutTarget > currentWorkers) {
            return executeScaleOut(scaleOutTarget, currentWorkers);
        }
        int scaleInTarget = policy.calculateScaleInTarget(score, currentWorkers);
        if (scaleInTarget < currentWorkers) {
            return executeScaleIn(scaleInTarget, currentWorkers);
        }
        return currentWorkers;
    }

    private int executeEmergencyScaleOut(int current) {
        Instant lastTime = lastEmergencyScaleOutTime.get();
        if (!isCooldownExpired(lastTime, EMERGENCY_COOLDOWN)) {
            LOG.debug("event=emergency_scale_out_cooldown_active");
            return current;
        }
        Instant now = clock.instant();
        if (!lastEmergencyScaleOutTime.compareAndSet(lastTime, now)) {
            // Another thread won the CAS race; skip to avoid over-scaling
            return current;
        }
        int target = current + 2;
        executor.scale(target);
        LOG.info("event=emergency_scale_out from={} to={}", current, target);
        return target;
    }

    private int executeScaleOut(int target, int current) {
        Instant lastTime = lastScaleOutTime.get();
        if (!isCooldownExpired(lastTime, SCALE_OUT_COOLDOWN)) {
            LOG.debug("event=scale_out_cooldown_active");
            return current;
        }
        Instant now = clock.instant();
        if (!lastScaleOutTime.compareAndSet(lastTime, now)) {
            return current;
        }
        executor.scale(target);
        LOG.info("event=scale_out from={} to={}", current, target);
        return target;
    }

    private int executeScaleIn(int target, int current) {
        Instant lastTime = lastScaleInTime.get();
        if (!isCooldownExpired(lastTime, SCALE_IN_COOLDOWN)) {
            LOG.debug("event=scale_in_cooldown_active");
            return current;
        }
        // G-RES-08: consult shuffle drain policy before scaling down
        ShuffleDrainPolicy.ScaleDownResult drainResult =
                shuffleDrainPolicy.prepareScaleDown(current, target);
        if (!drainResult.ready()) {
            LOG.warn("event=scale_in_blocked_by_shuffle reason={}", drainResult.reason());
            return current;
        }
        int adjustedTarget = drainResult.adjustedTarget();
        if (adjustedTarget >= current) {
            LOG.info("event=scale_in_skipped_after_shuffle_check adjusted={} current={}",
                    adjustedTarget, current);
            return current;
        }
        Instant now = clock.instant();
        if (!lastScaleInTime.compareAndSet(lastTime, now)) {
            return current;
        }
        executor.scale(adjustedTarget);
        if (adjustedTarget != target) {
            LOG.info("event=scale_in from={} to={} originalTarget={} shuffleAdjusted=true",
                    current, adjustedTarget, target);
        } else {
            LOG.info("event=scale_in from={} to={}", current, adjustedTarget);
        }
        return adjustedTarget;
    }

    private boolean isCooldownExpired(Instant lastTime, Duration cooldown) {
        return Duration.between(lastTime, clock.instant()).compareTo(cooldown) >= 0;
    }

    /**
     * Functional interface for executing scaling operations.
     */
    @FunctionalInterface
    public interface ScalingExecutor {

        /**
         * Scales the cluster to the target number of workers.
         * Must be idempotent: scaling to the same target twice is harmless.
         *
         * @param targetWorkers desired worker count
         */
        void scale(int targetWorkers);
    }
}
