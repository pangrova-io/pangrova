package org.pangrova.query.runtime.enums;

/** Outcome of waiting for a submitted query to publish its receipt. */
public enum SubmissionReadiness {
    READY, TIMED_OUT, INTERRUPTED
}
