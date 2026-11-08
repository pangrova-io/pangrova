package org.pangrova.materialization.domain;

/**
 * Lifecycle state of a materialization object header.
 */
public sealed interface ObjectState
        permits ObjectState.Creating, ObjectState.Active, ObjectState.Suspended,
        ObjectState.Failed, ObjectState.Dropping, ObjectState.Dropped {

    /**
     * Returns whether no further transition is possible.
     *
     * @return true for the dropped state
     */
    boolean isTerminal();

    /**
     * Returns whether this state may transition to the target state.
     *
     * @param target desired state
     * @return true when the transition is legal
     */
    boolean canTransitionTo(ObjectState target);

    /**
     * Returns the durable state label used by metadata and result rows.
     *
     * <p>Named {@code stateName} rather than {@code name} because the enum
     * constants inherit the final {@link Enum#name()} of their constant value.
     *
     * @return uppercase state name
     */
    default String stateName() {
        if (this == Creating.INSTANCE) {
            return "CREATING";
        }
        if (this == Active.INSTANCE) {
            return "ACTIVE";
        }
        if (this == Suspended.INSTANCE) {
            return "SUSPENDED";
        }
        if (this == Failed.INSTANCE) {
            return "FAILED";
        }
        if (this == Dropping.INSTANCE) {
            return "DROPPING";
        }
        return "DROPPED";
    }

    /** Object is being created; no published generation exists yet. */
    enum Creating implements ObjectState {
        INSTANCE;

        @Override
        public boolean isTerminal() {
            return false;
        }

        @Override
        public boolean canTransitionTo(ObjectState target) {
            return target == Active.INSTANCE || target == Failed.INSTANCE || target == Dropping.INSTANCE;
        }
    }

    /** Object has a published generation and accepts refreshes. */
    enum Active implements ObjectState {
        INSTANCE;

        @Override
        public boolean isTerminal() {
            return false;
        }

        @Override
        public boolean canTransitionTo(ObjectState target) {
            return target == Suspended.INSTANCE || target == Failed.INSTANCE || target == Dropping.INSTANCE;
        }
    }

    /** Object retains its published generation but schedules no refreshes. */
    enum Suspended implements ObjectState {
        INSTANCE;

        @Override
        public boolean isTerminal() {
            return false;
        }

        @Override
        public boolean canTransitionTo(ObjectState target) {
            return target == Active.INSTANCE || target == Dropping.INSTANCE;
        }
    }

    /** Object has no usable published generation and requires repair. */
    enum Failed implements ObjectState {
        INSTANCE;

        @Override
        public boolean isTerminal() {
            return false;
        }

        @Override
        public boolean canTransitionTo(ObjectState target) {
            return target == Active.INSTANCE || target == Dropping.INSTANCE;
        }
    }

    /** Drop is in progress; dependent objects are being removed first. */
    enum Dropping implements ObjectState {
        INSTANCE;

        @Override
        public boolean isTerminal() {
            return false;
        }

        @Override
        public boolean canTransitionTo(ObjectState target) {
            return target == Dropped.INSTANCE;
        }
    }

    /** Object is dropped; retained generations await reader-pin-aware collection. */
    enum Dropped implements ObjectState {
        INSTANCE;

        @Override
        public boolean isTerminal() {
            return true;
        }

        @Override
        public boolean canTransitionTo(ObjectState target) {
            return false;
        }
    }
}
