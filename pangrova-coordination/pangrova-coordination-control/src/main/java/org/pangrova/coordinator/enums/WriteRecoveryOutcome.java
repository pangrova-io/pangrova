package org.pangrova.coordinator.enums;

/** The terminal decision for a pending intent during recovery. */
public enum WriteRecoveryOutcome {
    /** The commit was applied (or confirmed already applied); mark it COMMITTED. */
    COMMITTED,
    /** The write is known not to have landed and cannot be safely retried; discard the intent. */
    ABORTED,
    /** The outcome is ambiguous; retain the intent and raise an alarm for manual reconciliation. */
    QUARANTINED
}
