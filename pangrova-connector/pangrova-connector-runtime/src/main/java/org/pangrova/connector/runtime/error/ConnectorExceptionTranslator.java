package org.pangrova.connector.runtime.error;

import org.pangrova.connector.api.error.ConnectorException;

import org.pangrova.common.api.annotation.Experimental;

import org.pangrova.common.exception.PangrovaErrorCode;

/**
 * Translates raw connector SDK exceptions into unified {@link ConnectorException}
 * with appropriate {@link PangrovaErrorCode} semantics at the connector boundary.
 *
 * <p>All connector implementations (Iceberg, Paimon, etc.) should use this
 * translator to ensure consistent error semantics across the system.</p>
 */
@Experimental
public final class ConnectorExceptionTranslator {

    private ConnectorExceptionTranslator() {}

    /**
     * Translates a raw exception from a connector SDK into a {@link ConnectorException}
     * with the appropriate unified error code.
     *
     * @param connectorName the connector name for context (e.g., "iceberg", "paimon")
     * @param operation     the operation being performed (e.g., "getSplits", "getMetadata")
     * @param cause         the raw exception from the connector SDK
     * @return a ConnectorException with unified error code
     */
    public static ConnectorException translate(String connectorName, String operation, Throwable cause) {
        PangrovaErrorCode code = classifyException(cause);
        String message = String.format("[%s] %s failed: %s", connectorName, operation, cause.getMessage());
        return new ConnectorException(code, message, cause);
    }

    /**
     * Classifies a raw exception into the appropriate unified error code based on
     * exception type and message patterns.
     */
    public static PangrovaErrorCode classifyException(Throwable cause) {
        if (cause == null) {
            return PangrovaErrorCode.CONNECTOR_READ_ERROR;
        }

        String className = cause.getClass().getName();
        String message = normalizedMessage(cause);

        if (isTableNotFound(className, message)) {
            return PangrovaErrorCode.CONNECTOR_TABLE_NOT_FOUND;
        }
        if (isTableAlreadyExists(className, message)) {
            return PangrovaErrorCode.CONNECTOR_TABLE_ALREADY_EXISTS;
        }
        if (isSchemaNotFound(className, message)) {
            return PangrovaErrorCode.CONNECTOR_SCHEMA_NOT_FOUND;
        }
        if (isPermissionDenied(className, message)) {
            return PangrovaErrorCode.CONNECTOR_PERMISSION_DENIED;
        }
        if (isTransientError(className, message)) {
            return PangrovaErrorCode.CONNECTOR_TRANSIENT_ERROR;
        }
        if (isWriteConflict(className, message)) {
            return PangrovaErrorCode.DML_CONFLICT_DETECTED;
        }
        return PangrovaErrorCode.CONNECTOR_READ_ERROR;
    }

    private static String normalizedMessage(Throwable cause) {
        return cause.getMessage() == null ? "" : cause.getMessage().toLowerCase();
    }

    private static boolean isTableNotFound(String className, String message) {
        return className.contains("NoSuchTableException")
                || className.contains("TableNotExistException")
                || className.contains("NoSuchNamespaceException")
                || className.contains("DatabaseNotExistException")
                || message.contains("table not found")
                || message.contains("does not exist");
    }

    private static boolean isTableAlreadyExists(String className, String message) {
        return className.contains("AlreadyExistsException")
                || className.contains("TableAlreadyExistException")
                || message.contains("already exists");
    }

    private static boolean isSchemaNotFound(String className, String message) {
        return className.contains("SchemaNotFoundException")
                || message.contains("schema not found")
                || message.contains("database not found");
    }

    private static boolean isPermissionDenied(String className, String message) {
        return className.contains("AccessDeniedException")
                || className.contains("ForbiddenException")
                || message.contains("permission denied")
                || message.contains("access denied")
                || message.contains("forbidden");
    }

    private static boolean isWriteConflict(String className, String message) {
        return className.contains("CommitFailedException")
                || className.contains("CommitConflictException")
                || message.contains("conflict")
                || message.contains("concurrent modification");
    }

    private static boolean isTransientError(String className, String message) {
        return className.contains("TimeoutException")
                || className.contains("ConnectException")
                || className.contains("SocketException")
                || className.contains("RetryableException")
                || message.contains("timeout")
                || message.contains("connection refused")
                || message.contains("connection reset")
                || message.contains("temporarily unavailable");
    }
}
