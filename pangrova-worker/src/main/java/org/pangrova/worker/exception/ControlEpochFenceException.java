package org.pangrova.worker.exception;

import org.pangrova.worker.enums.ControlEpochFenceReason;

/** Typed rejection raised when a task carries an invalid cluster-control term. */
public final class ControlEpochFenceException extends IllegalStateException {
    private static final long serialVersionUID = 1L;

    private final ControlEpochFenceReason reason;

    public ControlEpochFenceException(ControlEpochFenceReason reason, String message) {
        super(message);
        this.reason = reason;
    }

    public ControlEpochFenceReason reason() {
        return reason;
    }
}
