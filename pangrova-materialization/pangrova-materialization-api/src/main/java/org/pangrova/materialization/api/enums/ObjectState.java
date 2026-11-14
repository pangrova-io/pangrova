package org.pangrova.materialization.api.enums;

/**
 * Lifecycle state of a materialization object header.
 */
public enum ObjectState {
    /** Object is being created; no published generation exists yet. */
    CREATING,
    /** Object has a published generation and accepts refreshes. */
    ACTIVE,
    /** Object retains its published generation but schedules no refreshes. */
    SUSPENDED,
    /** Object has no usable published generation and requires repair. */
    FAILED,
    /** Drop is in progress; dependent objects are being removed first. */
    DROPPING,
    /** Object is dropped; retained generations await reader-pin-aware collection. */
    DROPPED;

    public boolean isTerminal() {
        return this == DROPPED;
    }

    /** Durable label used in metadata and SQL results. */
    public String stateName() {
        return name();
    }

    public boolean canTransitionTo(ObjectState target) {
        return switch (this) {
            case CREATING -> target == ACTIVE || target == FAILED || target == DROPPING;
            case ACTIVE -> target == SUSPENDED || target == FAILED || target == DROPPING;
            case SUSPENDED, FAILED -> target == ACTIVE || target == DROPPING;
            case DROPPING -> target == DROPPED;
            case DROPPED -> false;
        };
    }
}
