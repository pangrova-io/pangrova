package org.pangrova.shuffle.api.enums;

/** Readiness level for worker decommission. */
public enum DecommissionReadiness {
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
