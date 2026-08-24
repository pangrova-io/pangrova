package org.pangrova.connector.iceberg;

import org.pangrova.common.exception.PangrovaErrorCode;
import org.pangrova.spi.connector.base.ConnectorException;

/**
 * Thrown when an Iceberg catalog commit operation exceeds the configured timeout.
 * Contains the table identifier and elapsed time for diagnostics.
 *
 * <p>Callers should treat this as an IOException-equivalent for retry routing:
 * the commit may or may not have succeeded on the catalog side.
 */
final class CommitTimeoutException extends ConnectorException {

    private final String tableIdentifier;
    private final long elapsedMs;
    private final long timeoutMs;

    CommitTimeoutException(String tableIdentifier, long elapsedMs, long timeoutMs) {
        super(PangrovaErrorCode.CONNECTOR_TRANSIENT_ERROR,
                "Commit timed out for table %s after %dms (timeout=%dms)"
                        .formatted(tableIdentifier, elapsedMs, timeoutMs));
        this.tableIdentifier = tableIdentifier;
        this.elapsedMs = elapsedMs;
        this.timeoutMs = timeoutMs;
    }

    CommitTimeoutException(String tableIdentifier, long elapsedMs, long timeoutMs,
                           Throwable cause) {
        super(PangrovaErrorCode.CONNECTOR_TRANSIENT_ERROR,
                "Commit timed out for table %s after %dms (timeout=%dms)"
                        .formatted(tableIdentifier, elapsedMs, timeoutMs),
                cause);
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
