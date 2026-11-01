package org.pangrova.persistence.memory.query.transaction;

import org.junit.jupiter.api.Test;
import org.pangrova.spi.connector.coordination.WriteAuthorizationReceipt;
import org.pangrova.spi.connector.coordination.WriteCommitEntry;
import org.pangrova.spi.connector.coordination.WriteCommitFingerprint;
import org.pangrova.spi.connector.coordination.WriteDeclarationSnapshot;
import org.pangrova.spi.connector.metadata.table.QualifiedTableName;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class InMemoryWriteCommitJournalTest {
    @Test
    void confirmedAbortCannotBecomeCommitted() {
        var journal = new InMemoryWriteCommitJournal();
        var intent = intent();
        journal.recordIntent(intent);
        journal.markAborted(intent.queryId(), intent.commitFingerprint());
        assertThatThrownBy(() -> journal.markCommitted(intent.queryId())).isInstanceOf(IllegalStateException.class);
        assertThat(journal.getEntry(intent.queryId())).get()
                .extracting(WriteCommitEntry::state).isEqualTo(WriteCommitEntry.State.ABORTED);
    }

    @Test
    void admissionRequiresAPendingIntent() {
        var journal = new InMemoryWriteCommitJournal();
        var intent = intent();
        for (var state : List.of(WriteCommitEntry.State.COMMITTED, WriteCommitEntry.State.ABORTED)) {
            assertThatThrownBy(() -> journal.recordIntent(intent.withState(state, 2)))
                    .isInstanceOf(IllegalArgumentException.class);
        }
        assertThat(journal.getEntry(intent.queryId())).isEmpty();
    }

    @Test
    void repeatedCommitKeepsItsFirstTerminalObservation() {
        var journal = new InMemoryWriteCommitJournal();
        var intent = intent();
        journal.recordIntent(intent);
        journal.markCommitted(intent.queryId());
        var committed = journal.getEntry(intent.queryId()).orElseThrow();
        journal.markCommitted(intent.queryId());
        assertThat(journal.getEntry(intent.queryId())).containsSame(committed);
    }

    @Test
    void independentInstancesDoNotShareCommittedState() {
        var first = new InMemoryWriteCommitJournal();
        var second = new InMemoryWriteCommitJournal();
        var intent = intent();
        first.recordIntent(intent);
        first.markCommitted(intent.queryId());
        assertThat(second.getEntry(intent.queryId())).isEmpty();
    }

    private static WriteCommitEntry intent() {
        var table = new QualifiedTableName("catalog", "schema", "table");
        var declaration = new WriteDeclarationSnapshot(table, "0".repeat(64), "0".repeat(64), false);
        // Storage-only test: opaque receipt bytes are fingerprinted, never used as permission to execute.
        var receipt = new WriteAuthorizationReceipt(new byte[] {1}, declaration);
        var binding = new byte[] {2};
        List<byte[]> fragments = List.of(new byte[] {3});
        String fingerprint = WriteCommitFingerprint.compute(new WriteCommitFingerprint.Intent(
                "query", "tenant", "catalog", "schema.table", "INSERT", -1, binding,
                "principal", receipt, null), fragments);
        return new WriteCommitEntry("query", "catalog", "schema.table", WriteCommitEntry.State.PENDING,
                1, fragments, "INSERT", fingerprint, "tenant", "principal", receipt, -1, binding,
                fingerprint, null, null, 0, null, null);
    }
}
