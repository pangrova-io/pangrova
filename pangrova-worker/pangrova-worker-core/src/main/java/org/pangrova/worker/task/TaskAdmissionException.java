package org.pangrova.worker.task;

/** Fail-closed task admission rejection, before execution ownership is created. */
public final class TaskAdmissionException extends IllegalStateException {
    private static final long serialVersionUID = 1L;

    public enum Reason { EXPIRED, IDENTITY_CONFLICT, CAPACITY, CLOSED, SEALED }

    private final Reason reason;

    TaskAdmissionException(Reason reason, String message) {
        super(message);
        this.reason = reason;
    }

    /** Returns the admission rejection category for transport status mapping. */
    public Reason reason() {
        return reason;
    }
}
