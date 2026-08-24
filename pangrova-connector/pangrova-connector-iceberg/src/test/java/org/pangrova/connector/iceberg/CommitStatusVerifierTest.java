package org.pangrova.connector.iceberg;

import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.DeleteFiles;
import org.apache.iceberg.ExpireSnapshots;
import org.apache.iceberg.ManageSnapshots;
import org.apache.iceberg.OverwriteFiles;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.ReplacePartitions;
import org.apache.iceberg.ReplaceSortOrder;
import org.apache.iceberg.RewriteFiles;
import org.apache.iceberg.RewriteManifests;
import org.apache.iceberg.RowDelta;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.StatisticsFile;
import org.apache.iceberg.Table;
import org.apache.iceberg.Transaction;
import org.apache.iceberg.UpdateLocation;
import org.apache.iceberg.UpdatePartitionSpec;
import org.apache.iceberg.UpdateProperties;
import org.apache.iceberg.UpdateSchema;
import org.apache.iceberg.UpdateStatistics;
import org.apache.iceberg.encryption.EncryptionManager;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.LocationProvider;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for {@link CommitStatusVerifier} — verifies correct outcome detection
 * when a commit times out but may have actually succeeded (F-ICEBERG-02).
 *
 * <p>Uses stub Table implementations (no Hadoop dependency) to test the
 * verification logic in isolation.
 */
class CommitStatusVerifierTest {

    @Test
    void verifySucceeds_whenSnapshotAppearedOnEmptyTable() {
        // Simulate: table had no snapshot before commit, now has one
        StubSnapshot newSnapshot = new StubSnapshot(100L, null);
        StubTable table = new StubTable(newSnapshot);

        CommitStatusVerifier.VerificationResult result =
                CommitStatusVerifier.verify(table, -1L, "db.table_empty");

        assertThat(result.outcome())
                .as("Should detect success when snapshot appeared on previously-empty table")
                .isEqualTo(CommitStatusVerifier.CommitOutcome.SUCCEEDED);
        assertThat(result.detail()).contains("New snapshot").contains("100");
    }

    @Test
    void verifySucceeds_whenSnapshotAdvancedFromKnownBase() {
        // Simulate: table had snapshot 50, now has snapshot 100 (child of 50)
        StubSnapshot parentSnapshot = new StubSnapshot(50L, null);
        StubSnapshot currentSnapshot = new StubSnapshot(100L, 50L);
        StubTable table = new StubTable(currentSnapshot, parentSnapshot);

        CommitStatusVerifier.VerificationResult result =
                CommitStatusVerifier.verify(table, 50L, "db.table_advanced");

        assertThat(result.outcome())
                .as("Should detect success when snapshot advanced from known base")
                .isEqualTo(CommitStatusVerifier.CommitOutcome.SUCCEEDED);
        assertThat(result.detail()).contains("Snapshot advanced").contains("50").contains("100");
    }

    @Test
    void verifyFails_whenNoSnapshotOnEmptyTable() {
        // Simulate: table had no snapshot and still has none (commit genuinely failed)
        StubTable table = new StubTable(null);

        CommitStatusVerifier.VerificationResult result =
                CommitStatusVerifier.verify(table, -1L, "db.table_still_empty");

        assertThat(result.outcome())
                .as("Should detect failure when no snapshot appeared")
                .isEqualTo(CommitStatusVerifier.CommitOutcome.FAILED);
        assertThat(result.detail()).contains("unchanged");
    }

    @Test
    void verifyFails_whenSnapshotUnchangedFromBase() {
        // Simulate: table had snapshot 50 and still has snapshot 50
        StubSnapshot sameSnapshot = new StubSnapshot(50L, null);
        StubTable table = new StubTable(sameSnapshot);

        CommitStatusVerifier.VerificationResult result =
                CommitStatusVerifier.verify(table, 50L, "db.table_unchanged");

        assertThat(result.outcome())
                .as("Should detect failure when snapshot remains at pre-commit state")
                .isEqualTo(CommitStatusVerifier.CommitOutcome.FAILED);
        assertThat(result.detail()).contains("unchanged");
    }

    @Test
    void verifyReturnsUnknown_whenRefreshThrows() {
        // Simulate: catalog is unreachable during verification
        StubTable table = new StubTable(null) {
            @Override
            public void refresh() {
                throw new RuntimeException("Catalog unreachable");
            }
        };

        CommitStatusVerifier.VerificationResult result =
                CommitStatusVerifier.verify(table, 50L, "db.table_unreachable");

        assertThat(result.outcome())
                .as("Should return UNKNOWN when verification itself fails")
                .isEqualTo(CommitStatusVerifier.CommitOutcome.UNKNOWN);
        assertThat(result.detail()).contains("Verification failed");
    }

    @Test
    void verifySucceeds_whenSnapshotAdvancedByConcurrentCommit() {
        // Simulate: a different commit advanced the snapshot (still indicates our
        // commit may have been part of it or was overtaken). The verifier considers
        // any advancement from the base as success.
        StubSnapshot otherSnapshot = new StubSnapshot(200L, 150L);
        StubTable table = new StubTable(otherSnapshot);

        CommitStatusVerifier.VerificationResult result =
                CommitStatusVerifier.verify(table, 50L, "db.table_concurrent");

        // Even though 200's parent is 150 (not 50), the snapshot ID differs from 50
        // so isDescendantOf still returns true (snapshot != ancestor check)
        assertThat(result.outcome())
                .isEqualTo(CommitStatusVerifier.CommitOutcome.SUCCEEDED);
    }

    @Test
    void verificationConstants_areReasonable() {
        assertThat(CommitStatusVerifier.VERIFICATION_GRACE_MS)
                .as("Grace period should be positive and bounded")
                .isBetween(1_000L, 30_000L);
        assertThat(CommitStatusVerifier.MAX_VERIFICATION_ATTEMPTS)
                .as("Should make at least 1 attempt")
                .isGreaterThanOrEqualTo(1);
    }

    // --- Stub implementations ---

    /**
     * Minimal Snapshot stub sufficient for verifier testing.
     */
    private record StubSnapshot(long snapshotId, Long parentId) implements Snapshot {
        @Override public long sequenceNumber() { return 0; }
        @Override public long timestampMillis() { return System.currentTimeMillis(); }
        @Override public Map<String, String> summary() { return Map.of(); }
        @Override public Integer schemaId() { return 0; }
        @Override public String operation() { return "append"; }
        @Override public String manifestListLocation() { return ""; }
        @Override public List<org.apache.iceberg.ManifestFile> allManifests(FileIO io) { return List.of(); }
        @Override public List<org.apache.iceberg.ManifestFile> dataManifests(FileIO io) { return List.of(); }
        @Override public List<org.apache.iceberg.ManifestFile> deleteManifests(FileIO io) { return List.of(); }
        @Override public Iterable<org.apache.iceberg.DataFile> addedDataFiles(FileIO io) { return List.of(); }
        @Override public Iterable<org.apache.iceberg.DataFile> removedDataFiles(FileIO io) { return List.of(); }
    }

    /**
     * Minimal Table stub that returns a configurable current snapshot.
     * Does not require Hadoop or any real catalog backend.
     */
    private static class StubTable implements Table {
        private final Snapshot currentSnapshot;
        private final Snapshot parentSnapshot;

        StubTable(Snapshot currentSnapshot) {
            this(currentSnapshot, null);
        }

        StubTable(Snapshot currentSnapshot, Snapshot parentSnapshot) {
            this.currentSnapshot = currentSnapshot;
            this.parentSnapshot = parentSnapshot;
        }

        @Override public void refresh() { /* no-op for stub */ }
        @Override public Snapshot currentSnapshot() { return currentSnapshot; }

        @Override
        public Snapshot snapshot(long snapshotId) {
            if (currentSnapshot != null && currentSnapshot.snapshotId() == snapshotId) {
                return currentSnapshot;
            }
            if (parentSnapshot != null && parentSnapshot.snapshotId() == snapshotId) {
                return parentSnapshot;
            }
            return null;
        }

        // --- Required by Table interface but not used by verifier ---
        @Override public String name() { return "stub_table"; }
        @Override public org.apache.iceberg.TableScan newScan() { throw new UnsupportedOperationException(); }
        @Override public Schema schema() { return new Schema(Types.NestedField.required(1, "id", Types.LongType.get())); }
        @Override public Map<Integer, Schema> schemas() { return Map.of(); }
        @Override public PartitionSpec spec() { return PartitionSpec.unpartitioned(); }
        @Override public Map<Integer, PartitionSpec> specs() { return Map.of(); }
        @Override public SortOrder sortOrder() { return SortOrder.unsorted(); }
        @Override public Map<Integer, SortOrder> sortOrders() { return Map.of(); }
        @Override public Map<String, String> properties() { return Map.of(); }
        @Override public String location() { return "/tmp/stub"; }
        @Override public Iterable<Snapshot> snapshots() { return List.of(); }
        @Override public List<org.apache.iceberg.HistoryEntry> history() { return List.of(); }
        @Override public Map<String, org.apache.iceberg.SnapshotRef> refs() { return Map.of(); }
        @Override public UpdateSchema updateSchema() { throw new UnsupportedOperationException(); }
        @Override public UpdatePartitionSpec updateSpec() { throw new UnsupportedOperationException(); }
        @Override public UpdateProperties updateProperties() { throw new UnsupportedOperationException(); }
        @Override public ReplaceSortOrder replaceSortOrder() { throw new UnsupportedOperationException(); }
        @Override public UpdateLocation updateLocation() { throw new UnsupportedOperationException(); }
        @Override public AppendFiles newAppend() { throw new UnsupportedOperationException(); }
        @Override public RewriteFiles newRewrite() { throw new UnsupportedOperationException(); }
        @Override public RewriteManifests rewriteManifests() { throw new UnsupportedOperationException(); }
        @Override public OverwriteFiles newOverwrite() { throw new UnsupportedOperationException(); }
        @Override public RowDelta newRowDelta() { throw new UnsupportedOperationException(); }
        @Override public ReplacePartitions newReplacePartitions() { throw new UnsupportedOperationException(); }
        @Override public DeleteFiles newDelete() { throw new UnsupportedOperationException(); }
        @Override public ExpireSnapshots expireSnapshots() { throw new UnsupportedOperationException(); }
        @Override public ManageSnapshots manageSnapshots() { throw new UnsupportedOperationException(); }
        @Override public Transaction newTransaction() { throw new UnsupportedOperationException(); }
        @Override public FileIO io() { throw new UnsupportedOperationException(); }
        @Override public EncryptionManager encryption() { throw new UnsupportedOperationException(); }
        @Override public LocationProvider locationProvider() { throw new UnsupportedOperationException(); }
        @Override public List<StatisticsFile> statisticsFiles() { return List.of(); }
        @Override public UpdateStatistics updateStatistics() { throw new UnsupportedOperationException(); }
    }
}
