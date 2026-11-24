package org.pangrova.query.profiling.enums;

/** Outcome of one downstream read-preparation submission. */
public enum PreparationOutcome {
    QUEUED, RUNNING, COMPLETED, SKIPPED, FAILED, REJECTED
}
