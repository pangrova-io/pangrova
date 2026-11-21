package org.pangrova.connector.iceberg.exception;

/** The Iceberg snapshot is committed but its listener failed. */
public final class CommitNotificationException extends IllegalStateException {
    public CommitNotificationException(String catalogName, String schemaName, String tableName,
                        RuntimeException cause) {
        super("Commit notification failed for " + catalogName + "."
                + schemaName + "." + tableName, cause);
    }
}
