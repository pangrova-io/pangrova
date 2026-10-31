package org.pangrova.worker.dataset;

/** A metadata command cannot start after its absolute deadline. */
public final class ControlDeadlineExceededException extends IllegalArgumentException {
    ControlDeadlineExceededException() {
        super("Native dataset control deadline has expired");
    }
}
