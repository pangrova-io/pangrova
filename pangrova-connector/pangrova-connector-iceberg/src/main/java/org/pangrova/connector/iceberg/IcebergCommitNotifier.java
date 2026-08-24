package org.pangrova.connector.iceberg;

import org.pangrova.spi.connector.index.CommitEvent;
import org.pangrova.spi.connector.index.CommitEventType;
import org.pangrova.spi.connector.index.CommitListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.List;
import java.util.function.Consumer;

/**
 * Notifies the registered commit listener and split-cache invalidator after
 * successful Iceberg commits. Listener errors never fail the commit.
 */
final class IcebergCommitNotifier {

    private static final Logger LOG = LoggerFactory.getLogger(IcebergCommitNotifier.class);

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
     * Never fails the commit due to listener errors.
     *
     * @param schemaName   schema name
     * @param tableName    table name
     * @param addedFiles   data files added in this commit
     * @param removedFiles data files removed in this commit
     * @param snapshotId   the new snapshot ID
     * @param eventType    type of commit operation
     */
    void notifyCommit(String schemaName,
                      String tableName,
                      List<String> addedFiles,
                      List<String> removedFiles,
                      String snapshotId,
                      CommitEventType eventType) {
        Consumer<String> cacheInvalidator = this.splitCacheInvalidator;
        if (cacheInvalidator != null) {
            cacheInvalidator.accept(tableName);
        }

        CommitListener listener = this.commitListener;
        if (listener == null) {
            return;
        }
        try {
            CommitEvent event = new CommitEvent(
                    catalogName, schemaName, tableName,
                    addedFiles, removedFiles,
                    Instant.now(), snapshotId, eventType);
            listener.onCommit(event);
        } catch (Exception e) {
            LOG.warn("action=commit_listener_failed table={}.{}.{} eventType={} error={}",
                    catalogName, schemaName, tableName, eventType, e.getMessage());
        }
    }
}
