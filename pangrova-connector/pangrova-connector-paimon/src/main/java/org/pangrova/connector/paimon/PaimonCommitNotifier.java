package org.pangrova.connector.paimon;

import org.pangrova.spi.connector.index.CommitEvent;
import org.pangrova.spi.connector.index.CommitEventType;
import org.pangrova.spi.connector.index.CommitListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.List;

/**
 * Delivers {@link CommitEvent}s to the registered commit listener after
 * successful Paimon snapshot commits. Listener errors never fail the commit.
 */
final class PaimonCommitNotifier {

    private static final Logger LOG = LoggerFactory.getLogger(PaimonCommitNotifier.class);

    private final String catalogName;
    private volatile CommitListener listener;

    PaimonCommitNotifier(String catalogName) {
        this.catalogName = catalogName;
    }

    void register(CommitListener commitListener) {
        this.listener = commitListener;
    }

    /** Notifies the registered listener about a commit, defaulting to APPEND. */
    void notify(String schemaName, String tableName, List<String> addedFiles,
                List<String> removedFiles, String snapshotId) {
        notify(schemaName, tableName, addedFiles, removedFiles, snapshotId,
                CommitEventType.APPEND);
    }

    /** Notifies the registered listener about a commit with an explicit event type. */
    void notify(String schemaName, String tableName, List<String> addedFiles,
                List<String> removedFiles, String snapshotId, CommitEventType eventType) {
        CommitListener current = this.listener;
        if (current == null) {
            return;
        }
        try {
            CommitEvent event = new CommitEvent(
                    catalogName, schemaName, tableName,
                    addedFiles, removedFiles,
                    Instant.now(), snapshotId, eventType);
            current.onCommit(event);
        } catch (Exception e) {
            LOG.warn("action=commit_listener_failed table={}.{}.{} eventType={} error={}",
                    catalogName, schemaName, tableName, eventType, e.getMessage());
        }
    }
}
