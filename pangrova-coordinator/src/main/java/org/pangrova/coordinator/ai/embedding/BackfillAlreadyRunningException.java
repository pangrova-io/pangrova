package org.pangrova.coordinator.ai.embedding;

/**
 * Thrown when a backfill operation is requested for a table/column that already
 * has an active backfill in progress.
 */
public final class BackfillAlreadyRunningException extends RuntimeException {

    private final String tableName;
    private final String columnName;

    public BackfillAlreadyRunningException(String tableName, String columnName) {
        super("Backfill already running for table=" + tableName + " column=" + columnName);
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
