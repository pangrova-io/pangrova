package org.pangrova.materialization.api.enums;

/**
 * Scheduling state of a materialization object.
 */
public enum ScheduleState {

    /** Target lag is configured and the object is eligible for automatic refresh. */
    ACTIVE,

    /** Scheduling is paused by an operator; manual refresh remains possible. */
    SUSPENDED,

    /** No target lag is configured; the object refreshes only when asked. */
    DISABLED
}
