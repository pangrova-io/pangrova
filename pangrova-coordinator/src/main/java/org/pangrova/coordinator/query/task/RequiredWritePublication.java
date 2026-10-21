package org.pangrova.coordinator.query.task;

import org.pangrova.spi.connector.ConnectorSession;
import org.pangrova.spi.connector.metadata.ConnectorMetadata;
import org.pangrova.spi.connector.metadata.ConnectorOutputTableHandle;
import org.pangrova.spi.connector.metadata.DistributedWritePublicationMetadata;
import org.pangrova.spi.connector.metadata.NativeWriteFinalization;
import org.pangrova.spi.coordination.WriteCommitEntry;
import org.pangrova.spi.coordination.WriteCommitJournal;

/** Commits through the connector's actual publication authority for a durable downstream consumer. */
final class RequiredWritePublication {
    private RequiredWritePublication() {}

    static void commit(WriteCommitJournal journal, ConnectorMetadata metadata, ConnectorSession session,
            ConnectorOutputTableHandle handle, WriteCommitEntry intent) {
        boolean required = journal.requiresPublication(intent);
        if (required && metadata instanceof DistributedWritePublicationMetadata publisher) {
            var receipt = publisher.finishDistributedWriteWithPublication(
                    session, handle, intent.fragments(), intent.commitContext());
            if (receipt.outcome() != NativeWriteFinalization.Outcome.COMMITTED) {
                throw new IllegalStateException("Required write publication has not been confirmed");
            }
            journal.recordPublication(intent.queryId(), intent.commitFingerprint(), receipt);
        } else {
            if (required && handle.nativeWriteAllocation().isEmpty()) {
                throw new UnsupportedOperationException("Connector cannot confirm the required write publication");
            }
            metadata.finishDistributedWrite(session, handle, intent.fragments(), intent.commitContext());
        }
        if (required && journal.getEntry(intent.queryId()).orElseThrow().publication() == null) {
            throw new IllegalStateException("Required write commit lost its exact publication receipt");
        }
    }
}
