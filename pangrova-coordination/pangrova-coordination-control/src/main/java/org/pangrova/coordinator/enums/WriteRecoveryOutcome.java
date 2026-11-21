package org.pangrova.coordinator.enums;

/** The decision for a pending intent during recovery. */
public enum WriteRecoveryOutcome {
    /** The commit was applied (or confirmed already applied); mark it COMMITTED. */
    COMMITTED,
    /** The write is known not to have landed and cannot be safely retried; discard the intent. */
    ABORTED,
    /** The next recovery operation could not start because its Worker is unavailable. */
    DEFERRED,
    /** The outcome is ambiguous; retain the intent and raise an alarm for manual reconciliation. */
    QUARANTINED
}
