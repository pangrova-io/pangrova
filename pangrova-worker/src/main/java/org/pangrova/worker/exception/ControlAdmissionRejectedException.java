package org.pangrova.worker.exception;

/** Typed, retryable rejection when all metadata command slots are occupied. */
public final class ControlAdmissionRejectedException extends IllegalStateException {
    public ControlAdmissionRejectedException() {
        super("Native dataset control admission is full");
    }
}
