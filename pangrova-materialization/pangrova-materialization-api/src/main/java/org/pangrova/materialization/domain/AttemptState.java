package org.pangrova.materialization.domain;

/**
 * Lifecycle state of one refresh attempt.
 *
 * <p>Attempt identity and candidate binding are immutable. A retry creates a
 * new attempt; it never reuses or re-targets an existing one.
 */
public sealed interface AttemptState
        permits AttemptState.Preparing, AttemptState.Building, AttemptState.Committed,
        AttemptState.Published, AttemptState.Failed, AttemptState.Abandoned,
        AttemptState.Quarantined, AttemptState.Superseded {

    /**
     * Returns whether no further transition is possible without operator action.
     *
     * @return true for terminal states
     */
    boolean isTerminal();

    /**
     * Returns whether this state may transition to the target state.
     *
     * @param target desired state
     * @return true when the transition is legal
     */
    boolean canTransitionTo(AttemptState target);

    /**
     * Returns the durable state label used by metadata and result rows.
     *
     * @return uppercase state name
     */
    default String stateName() {
        if (this == Preparing.INSTANCE) {
            return "PREPARING";
        }
        if (this == Building.INSTANCE) {
            return "BUILDING";
        }
        if (this == Committed.INSTANCE) {
            return "COMMITTED";
        }
        if (this == Published.INSTANCE) {
            return "PUBLISHED";
        }
        if (this == Failed.INSTANCE) {
            return "FAILED";
        }
        if (this == Abandoned.INSTANCE) {
            return "ABANDONED";
        }
        if (this == Quarantined.INSTANCE) {
            return "QUARANTINED";
        }
        return "SUPERSEDED";
    }

    /** Durable reservation exists; the execution fence is being validated. */
    enum Preparing implements AttemptState {
        INSTANCE;

        @Override
        public boolean isTerminal() {
            return false;
        }

        @Override
        public boolean canTransitionTo(AttemptState target) {
            return target == Building.INSTANCE || target == Failed.INSTANCE
                    || target == Abandoned.INSTANCE;
        }
    }

    /** Native execution is running against the bound candidate table. */
    enum Building implements AttemptState {
        INSTANCE;

        @Override
        public boolean isTerminal() {
            return false;
        }

        @Override
        public boolean canTransitionTo(AttemptState target) {
            return target == Committed.INSTANCE || target == Failed.INSTANCE
                    || target == Abandoned.INSTANCE || target == Quarantined.INSTANCE;
        }
    }

    /** Native execution committed and exact evidence was recorded. */
    enum Committed implements AttemptState {
        INSTANCE;

        @Override
        public boolean isTerminal() {
            return false;
        }

        @Override
        public boolean canTransitionTo(AttemptState target) {
            return target == Published.INSTANCE || target == Quarantined.INSTANCE
                    || target == Superseded.INSTANCE || target == Failed.INSTANCE;
        }
    }

    /** The attempt's generation is the published head. */
    enum Published implements AttemptState {
        INSTANCE;

        @Override
        public boolean isTerminal() {
            return true;
        }

        @Override
        public boolean canTransitionTo(AttemptState target) {
            return false;
        }
    }

    /** Attempt failed before a durable commit was observed. */
    enum Failed implements AttemptState {
        INSTANCE;

        @Override
        public boolean isTerminal() {
            return true;
        }

        @Override
        public boolean canTransitionTo(AttemptState target) {
            return false;
        }
    }

    /** Attempt was abandoned by its owner before publication completed. */
    enum Abandoned implements AttemptState {
        INSTANCE;

        @Override
        public boolean isTerminal() {
            return true;
        }

        @Override
        public boolean canTransitionTo(AttemptState target) {
            return false;
        }
    }

    /** Evidence is missing or ambiguous; the candidate must be retained for reconciliation. */
    enum Quarantined implements AttemptState {
        INSTANCE;

        @Override
        public boolean isTerminal() {
            return false;
        }

        @Override
        public boolean canTransitionTo(AttemptState target) {
            return target == Committed.INSTANCE || target == Abandoned.INSTANCE;
        }
    }

    /** A newer attempt published first; this candidate is retained for garbage collection. */
    enum Superseded implements AttemptState {
        INSTANCE;

        @Override
        public boolean isTerminal() {
            return true;
        }

        @Override
        public boolean canTransitionTo(AttemptState target) {
            return false;
        }
    }
}
