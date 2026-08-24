package org.pangrova.worker.transport;

/**
 * Thrown when a transport ticket fails cryptographic validation.
 * Covers invalid signatures, expired tickets, and malformed ticket data.
 */
public final class TicketValidationException extends RuntimeException {

    public TicketValidationException(String message) {
        super(message);
    }

    public TicketValidationException(String message, Throwable cause) {
        super(message, cause);
    }
}
