package org.pangrova.coordinator.query.task;

import org.pangrova.connector.api.enums.NativeWriteCommitOutcome;
import org.pangrova.connector.api.session.ConnectorSession;
import org.pangrova.connector.api.metadata.ConnectorMetadata;
import org.pangrova.connector.api.metadata.ConnectorOutputTableHandle;
import org.pangrova.connector.api.metadata.DistributedWritePublicationMetadata;
import org.pangrova.persistence.query.api.commit.WriteCommitEntry;
import org.pangrova.persistence.query.api.commit.WriteCommitJournal;

/** Commits through the connector's actual publication authority for a durable downstream consumer. */
final class RequiredWritePublication {
    private RequiredWritePublication() {}

    static void commit(WriteCommitJournal journal, ConnectorMetadata metadata, ConnectorSession session,
            ConnectorOutputTableHandle handle, WriteCommitEntry intent, long currentToken, boolean required) {
        var context = metadata.requiresPublicationClaim() ? journal.publicationContext(intent, currentToken)
                : intent.commitContext();
        if (required && metadata instanceof DistributedWritePublicationMetadata publisher) {
            var receipt = publisher.finishDistributedWriteWithPublication(
                    session, handle, intent.fragments(), context);
            if (receipt.outcome() != NativeWriteCommitOutcome.COMMITTED) {
                throw new IllegalStateException("Required write publication has not been confirmed");
            }
            journal.recordPublication(intent.queryId(), intent.commitFingerprint(), receipt);
            return;
        }
        if (required && handle.nativeWriteAllocation().isEmpty()) {
            throw new UnsupportedOperationException("Connector cannot confirm the required write publication");
        }
        metadata.finishDistributedWrite(session, handle, intent.fragments(), context);
        if (required && journal.getEntry(intent.queryId()).orElseThrow().publication() == null) {
            throw new IllegalStateException("Required write commit lost its exact publication receipt");
        }
    }
}
