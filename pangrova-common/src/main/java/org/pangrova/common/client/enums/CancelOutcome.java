package org.pangrova.common.client.enums;

/** Logical cancellation outcome only; none of these values establishes physical execution or socket exit. */
public enum CancelOutcome {
    CANCEL_ACCEPTED,
    ALREADY_TERMINAL,
    NOT_FOUND
}
