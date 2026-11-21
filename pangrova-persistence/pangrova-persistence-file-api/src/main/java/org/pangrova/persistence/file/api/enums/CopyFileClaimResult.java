package org.pangrova.persistence.file.api.enums;

/** Durable ownership outcome for a COPY file claim. */
public enum CopyFileClaimResult {
    /** This execution durably acquired the file before dispatching any write. */
    ACQUIRED,
    /** The identical execution previously acquired the same immutable evidence. */
    SAME_EXECUTION,
    /** Another execution owns this file; its catalog outcome is not yet proven. */
    BLOCKED
}
