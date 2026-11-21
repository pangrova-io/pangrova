package org.pangrova.materialization.api.enums;

/**
 * Origin of a refresh attempt.
 */
public enum TriggerKind {

    /** Explicit {@code REFRESH} command. */
    MANUAL,

    /** Target-lag scheduler. */
    SCHEDULED,

    /** Reconciler or operator repair. */
    REPAIR,

    /** Initial creation refresh. */
    CREATION
}
