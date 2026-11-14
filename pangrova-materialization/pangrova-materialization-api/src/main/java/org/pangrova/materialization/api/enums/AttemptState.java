package org.pangrova.materialization.api.enums;

/**
 * Lifecycle state of one refresh attempt.
 *
 * <p>Attempt identity and candidate binding are immutable. A retry creates a
 * new attempt; it never reuses or re-targets an existing one.
 */
public enum AttemptState {
    /** Durable reservation exists; the execution fence is being validated. */
    PREPARING,
    /** Native execution is running against the bound candidate table. */
    BUILDING,
    /** Native execution committed and exact evidence was recorded. */
    COMMITTED,
    /** The attempt's generation is the published head. */
    PUBLISHED,
    /** Attempt failed before a durable commit was observed. */
    FAILED,
    /** Attempt was abandoned by its owner before publication completed. */
    ABANDONED,
    /** Evidence is missing or ambiguous; the candidate must be retained for reconciliation. */
    QUARANTINED,
    /** A newer attempt published first; this candidate is retained for garbage collection. */
    SUPERSEDED;

    public boolean isTerminal() {
        return this == PUBLISHED || this == FAILED || this == ABANDONED || this == SUPERSEDED;
    }

    /** Durable label used in metadata and SQL results. */
    public String stateName() {
        return name();
    }

    public boolean canTransitionTo(AttemptState target) {
        return switch (this) {
            case PREPARING -> target == BUILDING || target == FAILED || target == ABANDONED;
            case BUILDING -> target == COMMITTED || target == FAILED || target == ABANDONED || target == QUARANTINED;
            case COMMITTED -> target == PUBLISHED || target == QUARANTINED || target == SUPERSEDED || target == FAILED;
            case PUBLISHED, FAILED, ABANDONED, SUPERSEDED -> false;
            case QUARANTINED -> target == COMMITTED || target == ABANDONED;
        };
    }
}
