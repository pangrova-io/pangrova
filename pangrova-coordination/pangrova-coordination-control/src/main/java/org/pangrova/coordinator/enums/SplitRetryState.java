package org.pangrova.coordinator.enums;

/** Retry state of a single tracked split. */
public enum SplitRetryState {
    PENDING,
    RUNNING,
    DONE,
    FAILED,
    RETRY
}
