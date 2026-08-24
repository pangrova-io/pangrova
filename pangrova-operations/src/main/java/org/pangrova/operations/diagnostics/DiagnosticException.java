package org.pangrova.operations.diagnostics;

/**
 * Exception thrown when a diagnostic operation fails.
 */
public final class DiagnosticException extends Exception {

    public DiagnosticException(String message, Throwable cause) {
        super(message, cause);
    }

    public DiagnosticException(String message) {
        super(message);
    }
}
