package org.pangrova.coordinator.http.jdbc;

/**
 * Thrown when a JDBC query execution fails within the Avatica wire protocol layer.
 * Wraps errors from the coordinator query lifecycle into a form suitable for
 * Avatica error propagation back to the JDBC client.
 */
public final class JdbcExecutionException extends RuntimeException {

    public JdbcExecutionException(String message) {
        super(message);
    }

    public JdbcExecutionException(String message, Throwable cause) {
        super(message, cause);
    }
}
