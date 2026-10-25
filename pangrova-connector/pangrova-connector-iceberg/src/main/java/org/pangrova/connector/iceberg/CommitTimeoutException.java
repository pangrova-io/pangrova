package org.pangrova.connector.iceberg;

import org.apache.iceberg.exceptions.CommitStateUnknownException;

/**
 * Thrown when an Iceberg catalog commit operation exceeds the configured timeout.
 * Contains the table identifier and elapsed time for diagnostics.
 *
 * <p>Interruption does not stop a remote catalog commit. Keep the outcome unknown,
 * preserve staged files, and prohibit automatic retries until reconciliation.
 */
final class CommitTimeoutException extends CommitStateUnknownException {

    private final String tableIdentifier;
    private final long elapsedMs;
    private final long timeoutMs;

    CommitTimeoutException(String tableIdentifier, long elapsedMs, long timeoutMs,
                           Throwable cause) {
        super(new IllegalStateException(
                "Commit outcome unknown for table %s after %dms (timeout=%dms)"
                        .formatted(tableIdentifier, elapsedMs, timeoutMs), cause));
        this.tableIdentifier = tableIdentifier;
        this.elapsedMs = elapsedMs;
        this.timeoutMs = timeoutMs;
    }

    String tableIdentifier() {
        return tableIdentifier;
    }

    long elapsedMs() {
        return elapsedMs;
    }

    long timeoutMs() {
        return timeoutMs;
    }
}
