package org.pangrova.coordinator.streaming;

import org.pangrova.persistence.query.api.commit.CdcSourceIdentity;
import org.pangrova.persistence.query.api.commit.WriteCommitEntry;
import org.pangrova.persistence.query.api.commit.WriteCommitJournal;
import org.pangrova.persistence.query.api.enums.WriteCommitState;

import java.util.Objects;

/**
 * Orders durable CDC publication and source acknowledgement.
 *
 * <p>The journal intent is written before the connector commit. The source
 * offset is acknowledged only after the journal records COMMITTED. A retry
 * after an ambiguous publication reuses the same immutable fingerprint rather
 * than minting another intent.</p>
 */
public final class CdcWriteCommitCoordinator {

    /** Publishes the exact journaled fragments through the connector authority. */
    @FunctionalInterface
    public interface Publisher {
        void publish(WriteCommitEntry intent);
    }

    /** Acknowledges the exact admitted source interval after publication proof. */
    @FunctionalInterface
    public interface OffsetAcker {
        void acknowledge(CdcSourceIdentity source, String commitFingerprint);
    }

    private final WriteCommitJournal journal;
    private final Publisher publisher;
    private final OffsetAcker offsetAcker;

    public CdcWriteCommitCoordinator(
            WriteCommitJournal journal, Publisher publisher, OffsetAcker offsetAcker) {
        this.journal = Objects.requireNonNull(journal, "journal");
        this.publisher = Objects.requireNonNull(publisher, "publisher");
        this.offsetAcker = Objects.requireNonNull(offsetAcker, "offsetAcker");
    }

    /** Commits one immutable CDC intent and acknowledges its source only after proof. */
    public void commit(WriteCommitEntry intent) {
        Objects.requireNonNull(intent, "intent");
        CdcSourceIdentity source = Objects.requireNonNull(
                intent.cdcSource(), "CDC commit requires its exact source identity");
        var receipt = journal.getReceipt(intent.queryId()).orElse(null);
        if (receipt != null) {
            if (!receipt.cdcWrite()
                    || !receipt.commitFingerprint().equals(intent.commitFingerprint())
                    || receipt.state() != WriteCommitState.COMMITTED) {
                throw new IllegalStateException("CDC retry differs from its terminal write receipt");
            }
            offsetAcker.acknowledge(source, intent.commitFingerprint());
            return;
        }
        journal.recordIntent(intent);
        publisher.publish(intent);
        if (!journal.recordTerminalOutcome(intent, intent.executionFencingToken(), WriteCommitState.COMMITTED)) {
            throw new IllegalStateException("Lost execution anchor while publishing CDC intent: " + intent.queryId());
        }
        offsetAcker.acknowledge(source, intent.commitFingerprint());
    }
}
