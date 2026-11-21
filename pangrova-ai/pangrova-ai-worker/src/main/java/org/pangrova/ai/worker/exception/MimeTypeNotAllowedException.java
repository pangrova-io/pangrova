package org.pangrova.ai.worker.exception;

/**
 * Exception thrown when a MIME type violates the table's whitelist policy.
 */
public final class MimeTypeNotAllowedException extends RuntimeException {
    private static final long serialVersionUID = 1L;

    public MimeTypeNotAllowedException(String message) {
        super(message);
    }
}
