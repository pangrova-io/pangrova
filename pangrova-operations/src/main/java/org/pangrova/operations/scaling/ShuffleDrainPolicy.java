package org.pangrova.operations.scaling;

import java.time.Duration;
import java.util.Objects;

import org.pangrova.operations.scaling.ShuffleResidencyCheck.ScaleDownAdvice;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Coordinates shuffle data drain before worker removal during scale-down (G-RES-08).
 *
 * <p>When the auto-scaler decides to reduce the worker pool, this policy:
 * <ol>
 *   <li>Consults the {@link ShuffleResidencyCheck} to determine whether any workers
 *       targeted for removal hold live shuffle data.</li>
 *   <li>If drain is required, invokes the {@link DrainExecutor} to flush or migrate
 *       shuffle data within the configured timeout.</li>
 *   <li>Returns a {@link ScaleDownResult} indicating whether scale-down can proceed
 *       and with what target.</li>
 * </ol>
 *
 * <p>If drain fails or times out, the policy blocks scale-down to prevent shuffle data loss.
 * The auto-scaler can then retry on the next evaluation cycle or log a warning for operator
 * attention.
 *
 * <p>Industry context: Spark's external shuffle service (ESS) decouples shuffle data from
 * executors, allowing safe dynamic allocation. Celeborn and Uniffle serve the same role for
 * Spark/Flink. This policy provides equivalent protection for Pangrova's built-in local shuffle
 * before an external RSS is deployed.
 */
public final class ShuffleDrainPolicy {

    private static final Logger LOG = LoggerFactory.getLogger(ShuffleDrainPolicy.class);

    private final ShuffleResidencyCheck residencyCheck;
    private final DrainExecutor drainExecutor;
    private final Duration drainTimeout;

    /**
     * Creates a shuffle drain policy.
     *
     * @param residencyCheck checks shuffle data residency on workers
     * @param drainExecutor  executes the drain operation (flush/migrate shuffle data)
     * @param drainTimeout   maximum time to wait for drain to complete
     */
    public ShuffleDrainPolicy(
            ShuffleResidencyCheck residencyCheck,
            DrainExecutor drainExecutor,
            Duration drainTimeout) {
        this.residencyCheck = Objects.requireNonNull(residencyCheck, "residencyCheck");
        this.drainExecutor = Objects.requireNonNull(drainExecutor, "drainExecutor");
        this.drainTimeout = Objects.requireNonNull(drainTimeout, "drainTimeout");
        if (drainTimeout.isNegative() || drainTimeout.isZero()) {
            throw new IllegalArgumentException(
                    "drainTimeout must be positive, got: " + drainTimeout);
        }
    }

    /**
     * Prepares for scale-down by checking shuffle data residency and draining if needed.
     *
     * <p>Flow:
     * <ol>
     *   <li>Query the residency check for the proposed scale-down.</li>
     *   <li>If no drain is needed, return immediately with the safe target.</li>
     *   <li>If drain is needed, invoke the drain executor and wait up to {@code drainTimeout}.</li>
     *   <li>If drain succeeds, return ready with the safe target.</li>
     *   <li>If drain fails, return blocked (target stays at current).</li>
     * </ol>
     *
     * @param currentWorkers current number of workers
     * @param targetWorkers  desired target (must be less than currentWorkers)
     * @return result indicating whether scale-down can proceed and the adjusted target
     */
    public ScaleDownResult prepareScaleDown(int currentWorkers, int targetWorkers) {
        ScaleDownAdvice advice = residencyCheck.checkScaleDown(currentWorkers, targetWorkers);

        if (!advice.drainNeeded()) {
            LOG.debug("event=scale_down_check result=clear safeTarget={} reason={}",
                    advice.safeTarget(), advice.reason());
            return ScaleDownResult.ready(advice.safeTarget(), advice.reason());
        }

        LOG.info("event=scale_down_drain_required safeTarget={} timeout={}",
                advice.safeTarget(), drainTimeout);

        boolean drained = drainExecutor.drainAndWait(drainTimeout);
        if (drained) {
            LOG.info("event=scale_down_drain_complete safeTarget={}", advice.safeTarget());
            return ScaleDownResult.ready(advice.safeTarget(), "drain completed successfully");
        }

        LOG.warn("event=scale_down_drain_failed blocking scale-down to protect shuffle data");
        return ScaleDownResult.blocked(currentWorkers,
                "shuffle drain failed or timed out; scale-down blocked to prevent data loss");
    }

    /**
     * Creates a no-op drain policy that always allows scale-down.
     * Used when shuffle awareness is not configured.
     */
    public static ShuffleDrainPolicy noOp() {
        return new ShuffleDrainPolicy(
                ShuffleResidencyCheck.alwaysAllow(),
                timeout -> true,
                Duration.ofSeconds(1));
    }

    /**
     * Executes the drain of shuffle data from workers being decommissioned.
     *
     * <p>Implementations coordinate with the worker-side
     * {@link org.pangrova.shuffle.ShuffleDrainHandler} to flush pending
     * shuffle buffers to durable storage or migrate data to peer workers.
     */
    @FunctionalInterface
    public interface DrainExecutor {

        /**
         * Triggers drain of shuffle data on workers targeted for removal and waits
         * for completion.
         *
         * @param timeout maximum time to wait for drain
         * @return true if all shuffle data was successfully drained, false otherwise
         */
        boolean drainAndWait(Duration timeout);
    }

    /**
     * Result of a scale-down preparation.
     *
     * @param adjustedTarget the safe target worker count after shuffle data consideration
     * @param ready          true if scale-down can proceed, false if blocked
     * @param reason         explanation for observability and audit
     */
    public record ScaleDownResult(int adjustedTarget, boolean ready, String reason) {

        public ScaleDownResult {
            if (adjustedTarget < 0) {
                throw new IllegalArgumentException(
                        "adjustedTarget must be non-negative, got: " + adjustedTarget);
            }
            Objects.requireNonNull(reason, "reason must not be null");
        }

        /** Scale-down can proceed with the given target. */
        public static ScaleDownResult ready(int target, String reason) {
            return new ScaleDownResult(target, true, reason);
        }

        /** Scale-down is blocked; target remains at the current worker count. */
        public static ScaleDownResult blocked(int currentWorkers, String reason) {
            return new ScaleDownResult(currentWorkers, false, reason);
        }
    }
}
