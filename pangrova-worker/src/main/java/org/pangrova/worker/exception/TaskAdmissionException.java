package org.pangrova.worker.exception;

import org.pangrova.worker.enums.TaskAdmissionReason;

/** Fail-closed task admission rejection, before execution ownership is created. */
public final class TaskAdmissionException extends IllegalStateException {
    private static final long serialVersionUID = 1L;

    private final TaskAdmissionReason reason;

    public TaskAdmissionException(TaskAdmissionReason reason, String message) {
        super(message);
        this.reason = reason;
    }

    /** Returns the admission rejection category for transport status mapping. */
    public TaskAdmissionReason reason() {
        return reason;
    }
}
