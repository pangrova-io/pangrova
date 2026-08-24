package org.pangrova.coordinator.ai.embedding;

/**
 * Thrown when a backfill operation fails with an unrecoverable error.
 *
 * <p>Carries context about which table/column was being backfilled
 * to aid in diagnostics and structured error reporting.</p>
 */
public final class BackfillExecutionException extends RuntimeException {

    private final String tableName;
    private final String columnName;

    /** Wraps the underlying failure with the affected table and column context. */
    public BackfillExecutionException(String tableName, String columnName, String message, Throwable cause) {
        super("Backfill execution failed: table=" + tableName + ", column=" + columnName + ", reason=" + message,
                cause);
        this.tableName = tableName;
        this.columnName = columnName;
    }

    public String tableName() {
        return tableName;
    }

    public String columnName() {
        return columnName;
    }
}
