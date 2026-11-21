package org.pangrova.coordinator.enums;

/** Coordinator state for one scheduled task. */
public enum ScheduledTaskState {
    PENDING,
    SCHEDULED,
    RUNNING,
    FINISHED,
    FAILED,
    ABANDONED
}
