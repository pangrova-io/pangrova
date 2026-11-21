package org.pangrova.persistence.query.api.enums;

/** Ownership of the attempted write, not query execution or a durable commit receipt. */
public enum JournalAdmissionOutcome {
    REJECTED_BEFORE_TRANSFER,
    ACCEPTED_UNESTABLISHED
}
