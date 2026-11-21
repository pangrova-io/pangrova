package org.pangrova.coordinator.streaming;

import org.junit.jupiter.api.Test;
import org.pangrova.connector.api.commit.WriteAuthorizationReceipt;
import org.pangrova.connector.api.commit.WriteDeclarationSnapshot;
import org.pangrova.connector.api.metadata.table.QualifiedTableName;
import org.pangrova.persistence.query.api.commit.CdcSourceIdentity;
import org.pangrova.persistence.query.api.commit.WriteCommitEntry;
import org.pangrova.persistence.query.api.commit.WriteCommitFingerprint;
import org.pangrova.persistence.query.api.enums.WriteCommitState;
import org.pangrova.persistence.query.memory.transaction.InMemoryWriteCommitJournal;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class CdcWriteCommitCoordinatorTest {

    private static final CdcSourceIdentity SOURCE = new CdcSourceIdentity(
            "stream-1", "epoch-1",
            List.of(new CdcSourceIdentity.OffsetRange("orders", 0, 10, 20)));

    @Test
    void publishesThenMarksCommittedThenAcknowledgesTheSource() {
        var journal = new InMemoryWriteCommitJournal();
        List<String> events = new ArrayList<>();
        var coordinator = new CdcWriteCommitCoordinator(journal,
                intent -> events.add("publish"),
                (source, fingerprint) -> events.add("ack"));

        coordinator.commit(intent());

        assertThat(events).containsExactly("publish", "ack");
        assertThat(journal.isCommitted("q-cdc")).isTrue();
    }

    @Test
    void anAmbiguousPublicationNeverAcknowledgesTheSource() {
        var journal = new InMemoryWriteCommitJournal();
        int[] acks = {0};
        var coordinator = new CdcWriteCommitCoordinator(journal,
                intent -> {
                    throw new IllegalStateException("catalog publication ambiguous");
                },
                (source, fingerprint) -> acks[0]++);

        assertThatThrownBy(() -> coordinator.commit(intent()))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("ambiguous");
        assertThat(journal.isCommitted("q-cdc")).isFalse();
        assertThat(acks[0]).isZero();
    }

    @Test
    void aCommittedReplayDoesNotPublishAgain() {
        var journal = new InMemoryWriteCommitJournal();
        int[] publishes = {0};
        int[] acks = {0};
        var coordinator = new CdcWriteCommitCoordinator(journal,
                intent -> publishes[0]++, (source, fingerprint) -> acks[0]++);

        coordinator.commit(intent());
        coordinator.commit(intent());

        assertThat(publishes[0]).isEqualTo(1);
        assertThat(acks[0]).isEqualTo(2);
    }

    private static WriteCommitEntry intent() {
        var receipt = new WriteAuthorizationReceipt(new byte[]{1},
                new WriteDeclarationSnapshot(
                        new QualifiedTableName("catalog", "schema", "target"),
                        "0".repeat(64), "0".repeat(64), false));
        String fingerprint = WriteCommitFingerprint.compute(
                new WriteCommitFingerprint.Intent("q-cdc", "tenant-a", "catalog",
                        "schema.target", "INSERT", 42L, new byte[]{2}, "alice",
                        receipt, null, SOURCE),
                List.of(new byte[]{3}));
        return new WriteCommitEntry("q-cdc", "catalog", "schema.target",
                WriteCommitState.PENDING, 1_000L, List.of(new byte[]{3}), "INSERT",
                fingerprint, "tenant-a", "alice", receipt, 42L, new byte[]{2},
                fingerprint, null, null, 0L, null, null, SOURCE);
    }
}
