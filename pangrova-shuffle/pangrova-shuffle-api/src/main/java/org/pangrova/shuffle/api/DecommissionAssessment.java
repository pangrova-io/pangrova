package org.pangrova.shuffle.api;

import org.pangrova.shuffle.api.enums.DecommissionReadiness;

import java.util.List;

import org.pangrova.common.api.annotation.Experimental;

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
        DecommissionReadiness readiness,
        List<ShuffleDataRef> ephemeralData,
        long ephemeralBytes) {

    /** No removal guarantee is established; never interpreted as success or a timed drain. */
    public static final DecommissionAssessment UNKNOWN =
            new DecommissionAssessment(DecommissionReadiness.UNKNOWN, List.of(), 0);

    /** Advisory result for implementations that established complete, current evidence. */
    public static final DecommissionAssessment SAFE =
            new DecommissionAssessment(DecommissionReadiness.SAFE, List.of(), 0);

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
        return readiness == DecommissionReadiness.SAFE;
    }

    /** Returns whether the assessment supports removal after the required verified drain. */
    public boolean isSafeWithDrain() {
        return readiness == DecommissionReadiness.SAFE || readiness == DecommissionReadiness.SAFE_WITH_DRAIN;
    }

    /** Returns true if decommission would lose live shuffle data. */
    public boolean requiresReExecution() {
        return readiness == DecommissionReadiness.REQUIRES_RE_EXECUTION;
    }
}
