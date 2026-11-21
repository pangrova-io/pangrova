package org.pangrova.worker.exception;

/** A metadata command cannot start after its absolute deadline. */
public final class ControlDeadlineExceededException extends IllegalArgumentException {
    public ControlDeadlineExceededException() {
        super("Native dataset control deadline has expired");
    }
}
