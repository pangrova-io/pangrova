package org.pangrova.governance.lineage;

/**
 * Thrown when a lineage event cannot be delivered to the configured backend.
 */
public final class LineageTransportException extends RuntimeException {

    public LineageTransportException(String message, Throwable cause) {
        super(message, cause);
    }
}
