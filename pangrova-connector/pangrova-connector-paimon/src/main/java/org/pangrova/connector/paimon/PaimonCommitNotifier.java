package org.pangrova.connector.paimon;

import org.pangrova.connector.spi.index.CommitEvent;
import org.pangrova.connector.spi.index.CommitEventType;
import org.pangrova.connector.spi.index.CommitListener;

import java.time.Instant;
import java.util.List;

/**
 * Delivers {@link CommitEvent}s to the registered commit listener after
 * successful Paimon snapshot commits. Listener persistence failures surface
 * to the caller so journaled writes can replay the event.
 */
final class PaimonCommitNotifier {

    private final String catalogName;
    private volatile CommitListener listener;

    PaimonCommitNotifier(String catalogName) {
        this.catalogName = catalogName;
    }

    void register(CommitListener commitListener) {
        this.listener = commitListener;
    }

    /** Notifies the registered listener about a commit, defaulting to APPEND. */
    void notify(String tenantId, String schemaName, String tableName, List<String> addedFiles,
                List<String> removedFiles, String snapshotId) {
        notify(tenantId, schemaName, tableName, addedFiles, removedFiles, snapshotId,
                CommitEventType.APPEND);
    }

    /** Notifies the registered listener about a commit with an explicit event type. */
    void notify(String tenantId, String schemaName, String tableName, List<String> addedFiles,
                List<String> removedFiles, String snapshotId, CommitEventType eventType) {
        notify(tenantId, schemaName, tableName, addedFiles, removedFiles,
                snapshotId, eventType, null);
    }

    void notify(String tenantId, String schemaName, String tableName, List<String> addedFiles,
                List<String> removedFiles, String snapshotId, CommitEventType eventType,
                String commitFingerprint) {
        CommitListener current = this.listener;
        if (current == null) {
            return;
        }
        CommitEvent event = new CommitEvent(
                tenantId, catalogName, schemaName, tableName,
                addedFiles, removedFiles,
                Instant.now(), snapshotId, eventType, commitFingerprint);
        current.onCommit(event);
    }
}
