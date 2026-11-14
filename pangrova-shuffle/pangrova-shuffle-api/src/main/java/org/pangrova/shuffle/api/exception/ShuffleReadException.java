package org.pangrova.shuffle.api.exception;

import java.util.Objects;

/** Classifies partition read failure for execution policy without parsing provider messages. */
public class ShuffleReadException extends RuntimeException {
    public enum Reason { TRANSPORT, CORRUPTION }

    private final Reason reason;

    public ShuffleReadException(Reason reason, String message, Throwable cause) {
        super(message, cause);
        this.reason = Objects.requireNonNull(reason, "reason");
    }

    public Reason reason() { return reason; }
}
