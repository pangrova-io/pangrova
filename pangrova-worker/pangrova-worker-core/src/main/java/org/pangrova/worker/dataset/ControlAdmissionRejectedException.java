package org.pangrova.worker.dataset;

/** Typed, retryable rejection when all metadata command slots are occupied. */
public final class ControlAdmissionRejectedException extends IllegalStateException {
    ControlAdmissionRejectedException() {
        super("Native dataset control admission is full");
    }
}
