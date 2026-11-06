package org.pangrova.shuffle.api;

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
        Readiness readiness,
        List<ShuffleDataRef> ephemeralData,
        long ephemeralBytes) {

    /** Readiness level for worker decommission. */
    public enum Readiness {
        /** Inventory coverage, ownership or effective failure guarantee is not established. */
        UNKNOWN,

        /** Current complete, fenced evidence supports removal; not inferred from retention. */
        SAFE,

        /** Removal requires a verified drain while admission and evidence remain fenced. */
        SAFE_WITH_DRAIN,

        /**
         * Current evidence establishes that selected live outputs cannot survive removal.
         * Recovery must be authorized and completed, or removal remains blocked.
         */
        REQUIRES_RE_EXECUTION
    }

    /** No removal guarantee is established; never interpreted as success or a timed drain. */
    public static final DecommissionAssessment UNKNOWN =
            new DecommissionAssessment(Readiness.UNKNOWN, List.of(), 0);

    /** Advisory result for implementations that established complete, current evidence. */
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

    /** Returns whether the assessment supports removal after the required verified drain. */
    public boolean isSafeWithDrain() {
        return readiness == Readiness.SAFE || readiness == Readiness.SAFE_WITH_DRAIN;
    }

    /** Returns true if decommission would lose live shuffle data. */
    public boolean requiresReExecution() {
        return readiness == Readiness.REQUIRES_RE_EXECUTION;
    }
}
