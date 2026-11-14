package org.pangrova.worker.exception;

/** Typed rejection raised when a task carries an invalid cluster-control term. */
public final class ControlEpochFenceException extends IllegalStateException {
    private static final long serialVersionUID = 1L;

    public enum Reason {
        CONTROL_PLANE_NOT_READY,
        MISSING_EPOCH,
        INVALID_EPOCH,
        EPOCH_AHEAD,
        EPOCH_MISMATCH
    }

    private final Reason reason;

    public ControlEpochFenceException(Reason reason, String message) {
        super(message);
        this.reason = reason;
    }

    public Reason reason() {
        return reason;
    }
}
