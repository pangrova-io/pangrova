package org.pangrova.worker.enums;

/** Reason a Worker refused task admission. */
public enum TaskAdmissionReason {
    EXPIRED,
    IDENTITY_CONFLICT,
    CAPACITY,
    CLOSED,
    SEALED
}
