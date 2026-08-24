package org.pangrova.spi.coordination;

/**
 * Thrown when a journal write operation fails.
 * Wraps the underlying SQL or I/O exception with context.
 */
public final class JournalWriteException extends RuntimeException {

    public JournalWriteException(String message, Throwable cause) {
        super(message, cause);
    }
}
