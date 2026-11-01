package org.pangrova.spi.resource.scaling;

import java.util.Objects;

import org.pangrova.spi.annotation.Experimental;

/**
 * Checks shuffle data residency before scale-down decisions (G-RES-08).
 *
 * <p>The auto-scaler consults this interface before removing workers from the cluster.
 * Implementations bridge to the shuffle data inventory to determine whether the proposed
 * scale-down would lose live shuffle data, and whether a drain window can make it safe.
 *
 * <p>Without this check, scale-down of workers holding live ephemeral shuffle data forces
 * upstream stages to re-execute. By querying residency before scaling, the controller can:
 * <ul>
 *   <li>Proceed immediately when no live shuffle data is at risk</li>
 *   <li>Trigger a drain window when data can be persisted or migrated</li>
 *   <li>Adjust the scale-down target to avoid removing workers with non-drainable data</li>
 *   <li>Block scale-down entirely when data loss is unacceptable</li>
 * </ul>
 *
 * <p>The default in-tree implementation bridges to
 * {@link org.pangrova.shuffle.api.inventory.ShuffleDataInventory#assessDecommission(String)},
 * translating {@link org.pangrova.shuffle.api.inventory.DecommissionAssessment} into
 * scale-down advice.
 *
 * @see org.pangrova.shuffle.api.inventory.ShuffleDataInventory
 * @see org.pangrova.shuffle.api.inventory.DecommissionAssessment
 */
@Experimental(reason = "G-RES-08: shuffle data residency check for safe scale-down")
public interface ShuffleDataResidencyCheck {

    /**
     * Checks whether the proposed scale-down is safe with respect to shuffle data residency.
     *
     * <p>Implementations should examine the workers that would be removed (typically the
     * most recently added or least-loaded) and determine whether they hold live shuffle data
     * that would be lost on removal.
     *
     * @param currentWorkers current number of workers in the cluster
     * @param proposedTarget desired number of workers after scale-down (must be less than currentWorkers)
     * @return advice on whether and how to proceed with the scale-down
     */
    ScaleDownAdvice checkScaleDown(int currentWorkers, int proposedTarget);

    /**
     * Advice returned by a shuffle data residency check.
     *
     * @param safeTarget  the safe target worker count; may equal {@code proposedTarget} if scale-down
     *                    is fully safe, or may be higher if some workers cannot be safely removed
     * @param drainNeeded whether a drain window is required before removing workers
     * @param reason      human-readable explanation for observability and audit
     */
    record ScaleDownAdvice(int safeTarget, boolean drainNeeded, String reason) {

        public ScaleDownAdvice {
            if (safeTarget < 0) {
                throw new IllegalArgumentException(
                        "safeTarget must be non-negative, got: " + safeTarget);
            }
            Objects.requireNonNull(reason, "reason must not be null");
        }

        /** Creates advice that allows the proposed target without drain. */
        public static ScaleDownAdvice allow(int target) {
            return new ScaleDownAdvice(target, false, "no live shuffle data at risk");
        }

        /** Creates advice that allows the proposed target but requires drain first. */
        public static ScaleDownAdvice allowWithDrain(int target) {
            return new ScaleDownAdvice(target, true,
                    "live shuffle data requires drain before removal");
        }

        /**
         * Creates advice that adjusts the target upward to protect workers with live shuffle data.
         *
         * @param adjustedTarget the adjusted (higher) target, protecting blocked workers
         * @param reason         explanation of why the target was adjusted
         */
        public static ScaleDownAdvice adjust(int adjustedTarget, String reason) {
            return new ScaleDownAdvice(adjustedTarget, false, reason);
        }
    }

    /**
     * Returns a residency check that always allows scale-down without drain.
     * Used when shuffle awareness is not configured or when all shuffle data is
     * in a durable external store.
     */
    static ShuffleDataResidencyCheck alwaysAllow() {
        return (currentWorkers, proposedTarget) -> ScaleDownAdvice.allow(proposedTarget);
    }
}
