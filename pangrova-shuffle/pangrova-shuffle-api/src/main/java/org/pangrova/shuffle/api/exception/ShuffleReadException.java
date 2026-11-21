package org.pangrova.shuffle.api.exception;

import org.pangrova.shuffle.api.enums.ShuffleReadFailureReason;

import java.util.Objects;

/** Classifies partition read failure for execution policy without parsing provider messages. */
public class ShuffleReadException extends RuntimeException {

    private final ShuffleReadFailureReason reason;

    public ShuffleReadException(ShuffleReadFailureReason reason, String message, Throwable cause) {
        super(message, cause);
        this.reason = Objects.requireNonNull(reason, "reason");
    }

    public ShuffleReadFailureReason reason() { return reason; }
}
