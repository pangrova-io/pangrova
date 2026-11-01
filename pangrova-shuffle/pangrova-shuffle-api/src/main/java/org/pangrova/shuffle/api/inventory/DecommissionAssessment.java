package org.pangrova.shuffle.api.inventory;

import java.util.List;

import org.pangrova.spi.annotation.Experimental;

import java.util.Objects;

/**
 * Result of assessing whether a worker can be safely decommissioned with respect to
 * its shuffle data (G-RES-08).
 *
 * <p>Returned by {@link ShuffleDataInventory#assessDecommission(String)} to guide the
 * coordinator's scale-down logic.
 *
 * @param readiness         the decommission readiness level
 * @param ephemeralData     live EPHEMERAL shuffle data that would be lost; empty when safe
 * @param ephemeralBytes    total bytes of ephemeral data at risk
 */
@Experimental(reason = "G-RES-08: decommission assessment for safe scale-down")
public record DecommissionAssessment(
        Readiness readiness,
        List<ShuffleDataRef> ephemeralData,
        long ephemeralBytes) {

    /** Readiness level for worker decommission. */
    public enum Readiness {
        /** No live shuffle data, or all data is PERSISTENT. Immediate scale-down is safe. */
        SAFE,

        /** Worker has DETACHED shuffle data. Safe to decommission after drain window. */
        SAFE_WITH_DRAIN,

        /**
         * Worker has live EPHEMERAL shuffle data. Decommissioning will lose data —
         * affected upstream stages must be re-executed, or scale-down should be blocked.
         */
        REQUIRES_RE_EXECUTION
    }

    /** Sentinel for a safe-to-decommission worker. */
    public static final DecommissionAssessment SAFE =
            new DecommissionAssessment(Readiness.SAFE, List.of(), 0);

    public DecommissionAssessment {
        Objects.requireNonNull(readiness, "readiness must not be null");
        Objects.requireNonNull(ephemeralData, "ephemeralData must not be null");
        ephemeralData = List.copyOf(ephemeralData);
        if (ephemeralBytes < 0) {
            throw new IllegalArgumentException(
                    "ephemeralBytes must be non-negative, got: " + ephemeralBytes);
        }
    }

    /** Returns true if the worker can be decommissioned without losing shuffle data. */
    public boolean isSafe() {
        return readiness == Readiness.SAFE;
    }

    /** Returns true if the worker can be decommissioned after a drain period. */
    public boolean isSafeWithDrain() {
        return readiness == Readiness.SAFE || readiness == Readiness.SAFE_WITH_DRAIN;
    }

    /** Returns true if decommission would lose live shuffle data. */
    public boolean requiresReExecution() {
        return readiness == Readiness.REQUIRES_RE_EXECUTION;
    }
}
