package org.pangrova.operations.scaling;

import java.util.Objects;

/**
 * Module-local contract for checking shuffle data residency before scale-down.
 *
 * <p>Kept module-local so pangrova-operations does not depend on the shuffle
 * runtime; production wiring bridges the concrete check to this interface.
 */
@FunctionalInterface
public interface ShuffleResidencyCheck {

    /**
     * Checks whether the proposed scale-down is safe with respect to shuffle data.
     *
     * @param currentWorkers current number of workers
     * @param proposedTarget desired number of workers after scale-down
     * @return advice on whether and how to proceed
     */
    ScaleDownAdvice checkScaleDown(int currentWorkers, int proposedTarget);

    /**
     * Advice returned by a shuffle data residency check.
     *
     * @param safeTarget  the safe target worker count
     * @param drainNeeded whether a drain window is required before removing workers
     * @param reason      human-readable explanation for observability
     */
    record ScaleDownAdvice(int safeTarget, boolean drainNeeded, String reason) {

        public ScaleDownAdvice {
            if (safeTarget < 0) {
                throw new IllegalArgumentException(
                        "safeTarget must be non-negative, got: " + safeTarget);
            }
            Objects.requireNonNull(reason, "reason must not be null");
        }

        /** Allows the proposed target without drain. */
        public static ScaleDownAdvice allow(int target) {
            return new ScaleDownAdvice(target, false, "no live shuffle data at risk");
        }

        /** Allows the proposed target but requires drain first. */
        public static ScaleDownAdvice allowWithDrain(int target) {
            return new ScaleDownAdvice(target, true,
                    "live shuffle data requires drain before removal");
        }

        /** Adjusts the target with no drain needed and a custom reason. */
        public static ScaleDownAdvice adjust(int adjustedTarget, String reason) {
            return new ScaleDownAdvice(adjustedTarget, false, reason);
        }
    }
}
