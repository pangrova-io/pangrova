package org.pangrova.materialization.api.domain;

/**
 * Lifecycle state of a physical generation.
 */
public sealed interface GenerationState
        permits GenerationState.Candidate, GenerationState.Committed, GenerationState.Published,
        GenerationState.Retired, GenerationState.Quarantined, GenerationState.Collected {

    /**
     * Returns whether the generation can never become query-visible.
     *
     * @return true for collected generations
     */
    boolean isTerminal();

    /**
     * Returns whether this state may transition to the target state.
     *
     * @param target desired state
     * @return true when the transition is legal
     */
    boolean canTransitionTo(GenerationState target);

    /** Physical table is reserved but not yet proven committed. */
    enum Candidate implements GenerationState {
        INSTANCE;

        @Override
        public boolean isTerminal() {
            return false;
        }

        @Override
        public boolean canTransitionTo(GenerationState target) {
            return target == Committed.INSTANCE || target == Quarantined.INSTANCE
                    || target == Collected.INSTANCE;
        }
    }

    /** Exact commit evidence exists; publication has not completed. */
    enum Committed implements GenerationState {
        INSTANCE;

        @Override
        public boolean isTerminal() {
            return false;
        }

        @Override
        public boolean canTransitionTo(GenerationState target) {
            return target == Published.INSTANCE || target == Quarantined.INSTANCE
                    || target == Collected.INSTANCE;
        }
    }

    /** Generation is the published head for its incarnation. */
    enum Published implements GenerationState {
        INSTANCE;

        @Override
        public boolean isTerminal() {
            return false;
        }

        @Override
        public boolean canTransitionTo(GenerationState target) {
            return target == Retired.INSTANCE;
        }
    }

    /** Generation was superseded by a newer head but may still be pinned by readers. */
    enum Retired implements GenerationState {
        INSTANCE;

        @Override
        public boolean isTerminal() {
            return false;
        }

        @Override
        public boolean canTransitionTo(GenerationState target) {
            return target == Collected.INSTANCE;
        }
    }

    /** Commit evidence is ambiguous; the physical table must not be collected automatically. */
    enum Quarantined implements GenerationState {
        INSTANCE;

        @Override
        public boolean isTerminal() {
            return false;
        }

        @Override
        public boolean canTransitionTo(GenerationState target) {
            return target == Committed.INSTANCE || target == Collected.INSTANCE;
        }
    }

    /** Physical table was dropped after reader pins drained. */
    enum Collected implements GenerationState {
        INSTANCE;

        @Override
        public boolean isTerminal() {
            return true;
        }

        @Override
        public boolean canTransitionTo(GenerationState target) {
            return false;
        }
    }
}
