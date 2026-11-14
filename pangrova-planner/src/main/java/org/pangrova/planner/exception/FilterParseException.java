package org.pangrova.planner.exception;

/** Exception thrown when a filter expression cannot be parsed or converted. */
public final class FilterParseException extends RuntimeException {
    public FilterParseException(String message, Throwable cause) {
        super(message, cause);
    }
}
