package org.pangrova.planner.exception;

/** Exception thrown when a mask expression cannot be parsed or converted. */
public final class MaskParseException extends RuntimeException {
    public MaskParseException(String message, Throwable cause) {
        super(message, cause);
    }
}
