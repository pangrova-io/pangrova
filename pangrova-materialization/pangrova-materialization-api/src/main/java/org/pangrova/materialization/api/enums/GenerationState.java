package org.pangrova.materialization.api.enums;

/**
 * Lifecycle state of a physical generation.
 */
public enum GenerationState {
    /** Physical table is reserved but not yet proven committed. */
    CANDIDATE,
    /** Exact commit evidence exists; publication has not completed. */
    COMMITTED,
    /** Generation is the published head for its incarnation. */
    PUBLISHED,
    /** Generation was superseded by a newer head but may still be pinned by readers. */
    RETIRED,
    /** Commit evidence is ambiguous; the physical table must not be collected automatically. */
    QUARANTINED,
    /** Physical table was dropped after reader pins drained. */
    COLLECTED;

    public boolean isTerminal() {
        return this == COLLECTED;
    }

    /** Returns whether a generation may move from this state to {@code target}. */
    public boolean canTransitionTo(GenerationState target) {
        return switch (this) {
            case CANDIDATE -> target == COMMITTED || target == QUARANTINED || target == COLLECTED;
            case COMMITTED -> target == PUBLISHED || target == QUARANTINED || target == COLLECTED;
            case PUBLISHED -> target == RETIRED;
            case RETIRED -> target == COLLECTED;
            case QUARANTINED -> target == COMMITTED || target == COLLECTED;
            case COLLECTED -> false;
        };
    }
}
