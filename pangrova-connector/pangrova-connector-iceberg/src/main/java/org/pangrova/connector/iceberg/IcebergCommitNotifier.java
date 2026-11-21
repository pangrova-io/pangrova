package org.pangrova.connector.iceberg;

import org.pangrova.connector.iceberg.exception.CommitNotificationException;

import org.pangrova.connector.api.index.CommitEvent;
import org.pangrova.connector.api.enums.CommitEventType;
import org.pangrova.connector.api.index.CommitListener;

import java.time.Instant;
import java.util.List;
import java.util.function.Consumer;

/**
 * Notifies the registered commit listener and split-cache invalidator after
 * successful Iceberg commits. Persistence failures surface to the caller so
 * journaled writes can replay the event after an ambiguous commit outcome.
 */
final class IcebergCommitNotifier {

    private final String catalogName;
    private volatile CommitListener commitListener;
    private volatile Consumer<String> splitCacheInvalidator;

    IcebergCommitNotifier(String catalogName) {
        this.catalogName = catalogName;
    }

    void registerCommitListener(CommitListener listener) {
        this.commitListener = listener;
    }

    /**
     * Registers a callback to invalidate the split manager's manifest cache
     * when a commit occurs. The callback receives the table name.
     */
    void setSplitCacheInvalidator(Consumer<String> invalidator) {
        this.splitCacheInvalidator = invalidator;
    }

    /**
     * Notifies the registered commit listener after a successful Iceberg commit.
     * Propagates listener errors; the physical snapshot may already be committed.
     *
     * @param schemaName   schema name
     * @param tableName    table name
     * @param addedFiles   data files added in this commit
     * @param removedFiles data files removed in this commit
     * @param snapshotId   the new snapshot ID
     * @param eventType    type of commit operation
     */
    void notifyCommit(String tenantId,
                      String schemaName,
                      String tableName,
                      List<String> addedFiles,
                      List<String> removedFiles,
                      String snapshotId,
                      CommitEventType eventType) {
        notifyCommit(tenantId, schemaName, tableName, addedFiles, removedFiles,
                snapshotId, eventType, null);
    }

    void notifyCommit(String tenantId,
                      String schemaName,
                      String tableName,
                      List<String> addedFiles,
                      List<String> removedFiles,
                      String snapshotId,
                      CommitEventType eventType,
                      String commitFingerprint) {
        try {
            Consumer<String> cacheInvalidator = this.splitCacheInvalidator;
            if (cacheInvalidator != null) {
                cacheInvalidator.accept(tableName);
            }
            CommitListener listener = this.commitListener;
            if (listener != null) {
                listener.onCommit(new CommitEvent(
                        tenantId, catalogName, schemaName, tableName,
                        addedFiles, removedFiles,
                        Instant.now(), snapshotId, eventType, commitFingerprint));
            }
        } catch (RuntimeException failure) {
            throw new CommitNotificationException(catalogName, schemaName, tableName, failure);
        }
    }
}
