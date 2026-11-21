package org.pangrova.connector.iceberg.exception;

import org.apache.iceberg.exceptions.CommitStateUnknownException;

/**
 * Thrown when an Iceberg catalog commit operation exceeds the configured timeout.
 * Contains the table identifier and elapsed time for diagnostics.
 *
 * <p>Interruption does not stop a remote catalog commit. Keep the outcome unknown,
 * preserve staged files, and prohibit automatic retries until reconciliation.
 */
public final class CommitTimeoutException extends CommitStateUnknownException {

    private final String tableIdentifier;
    private final long elapsedMs;
    private final long timeoutMs;

    /** Reports a commit whose outcome is unknown after the timeout; the caller must not assume failure. */
    public CommitTimeoutException(String tableIdentifier, long elapsedMs, long timeoutMs,
                           Throwable cause) {
        super(new IllegalStateException(
                "Commit outcome unknown for table %s after %dms (timeout=%dms)"
                        .formatted(tableIdentifier, elapsedMs, timeoutMs), cause));
        this.tableIdentifier = tableIdentifier;
        this.elapsedMs = elapsedMs;
        this.timeoutMs = timeoutMs;
    }

    public String tableIdentifier() {
        return tableIdentifier;
    }

    public long elapsedMs() {
        return elapsedMs;
    }

    public long timeoutMs() {
        return timeoutMs;
    }
}
