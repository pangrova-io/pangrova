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
import org.pangrova.spi.connector.metadata.DistributedWriteCommitContext;

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
    void verifyUnknown_whenSnapshotAppearedOnPreviouslyEmptyTableWithoutContext() {
        // Simulate: table had no snapshot before commit, now has one. Without a durable
        // marker the new snapshot could belong to any writer, so it is not proof.
        StubSnapshot newSnapshot = new StubSnapshot(100L, null);
        StubTable table = new StubTable(newSnapshot);

        CommitStatusVerifier.VerificationResult result =
                CommitStatusVerifier.verify(table, -1L, "db.table_empty");

        assertThat(result.outcome())
                .as("An unattributable new snapshot must fail closed as UNKNOWN")
                .isEqualTo(CommitStatusVerifier.CommitOutcome.UNKNOWN);
        assertThat(result.detail()).contains("advanced");
    }

    @Test
    void verifyUnknown_whenSnapshotAdvancedWithoutContext() {
        // Simulate: table had snapshot 50, now has snapshot 100 (child of 50).
        // A concurrent writer's commit descends from the same base, so lineage alone
        // cannot attribute the snapshot to our commit.
        StubSnapshot parentSnapshot = new StubSnapshot(50L, null);
        StubSnapshot currentSnapshot = new StubSnapshot(100L, 50L);
        StubTable table = new StubTable(currentSnapshot, parentSnapshot);

        CommitStatusVerifier.VerificationResult result =
                CommitStatusVerifier.verify(table, 50L, "db.table_advanced");

        assertThat(result.outcome())
                .as("Lineage without a commit marker must fail closed as UNKNOWN")
                .isEqualTo(CommitStatusVerifier.CommitOutcome.UNKNOWN);
        assertThat(result.detail()).contains("advanced");
    }

    @Test
    void verifyUnknown_whenNoSnapshotOnEmptyTable() {
        // Simulate: table had no snapshot and still has none (remote commit may still be in flight)
        StubTable table = new StubTable(null);

        CommitStatusVerifier.VerificationResult result =
                CommitStatusVerifier.verify(table, -1L, "db.table_still_empty");

        assertThat(result.outcome())
                .as("No snapshot cannot prove the remote commit has stopped")
                .isEqualTo(CommitStatusVerifier.CommitOutcome.UNKNOWN);
        assertThat(result.detail()).contains("unchanged");
    }

    @Test
    void verifyUnknown_whenSnapshotUnchangedFromBase() {
        // Simulate: table had snapshot 50 and still has snapshot 50
        StubSnapshot sameSnapshot = new StubSnapshot(50L, null);
        StubTable table = new StubTable(sameSnapshot);

        CommitStatusVerifier.VerificationResult result =
                CommitStatusVerifier.verify(table, 50L, "db.table_unchanged");

        assertThat(result.outcome())
                .as("An unchanged snapshot cannot prove the remote commit has stopped")
                .isEqualTo(CommitStatusVerifier.CommitOutcome.UNKNOWN);
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
    void verifyUnknown_whenSnapshotAdvancedWithoutProvableLineage() {
        // A concurrent writer advanced the snapshot but we cannot prove our commit is
        // in its ancestry. An advanced snapshot ID alone is not proof of success.
        StubSnapshot otherSnapshot = new StubSnapshot(200L, 150L);
        StubTable table = new StubTable(otherSnapshot);

        CommitStatusVerifier.VerificationResult result =
                CommitStatusVerifier.verify(table, 50L, "db.table_concurrent");

        assertThat(result.outcome())
                .as("Unprovable advancement must fail closed as UNKNOWN")
                .isEqualTo(CommitStatusVerifier.CommitOutcome.UNKNOWN);
        assertThat(result.detail()).contains("advanced");
    }

    @Test
    void verifySucceeds_whenCommitMarkerIsInCurrentAncestry() {
        String fingerprint = "a".repeat(64);
        StubSnapshot markerSnapshot = new StubSnapshot(100L, null,
                markerSummary(fingerprint, -1L));
        StubTable table = new StubTable(markerSnapshot);

        CommitStatusVerifier.VerificationResult result = CommitStatusVerifier.verify(
                table, -1L, context(fingerprint, -1L), "db.table_marker");

        assertThat(result.outcome())
                .as("The durable commit marker is positive proof of success")
                .isEqualTo(CommitStatusVerifier.CommitOutcome.SUCCEEDED);
        assertThat(result.detail()).contains("marker");
    }

    @Test
    void verifyUnknown_whenAdvancedSnapshotLacksCommitMarker() {
        String fingerprint = "b".repeat(64);
        StubSnapshot advanced = new StubSnapshot(200L, 150L);
        StubTable table = new StubTable(advanced);

        CommitStatusVerifier.VerificationResult result = CommitStatusVerifier.verify(
                table, 50L, context(fingerprint, 50L), "db.table_no_marker");

        assertThat(result.outcome())
                .as("A snapshot that advanced without our marker cannot prove our commit")
                .isEqualTo(CommitStatusVerifier.CommitOutcome.UNKNOWN);
    }

    private static Map<String, String> markerSummary(String fingerprint, long baseSnapshotId) {
        return Map.of(
                IcebergDistributedCommitMarker.FINGERPRINT, fingerprint,
                IcebergDistributedCommitMarker.EXECUTION_ID, "exec-1",
                IcebergDistributedCommitMarker.TENANT_ID, "tenant-1",
                IcebergDistributedCommitMarker.BASE_SNAPSHOT_ID,
                String.valueOf(baseSnapshotId));
    }

    private static DistributedWriteCommitContext context(String fingerprint, long baseSnapshotId) {
        return new DistributedWriteCommitContext("exec-1", "tenant-1", baseSnapshotId,
                new byte[] {1}, fingerprint, "alice",
                IcebergWriteReceiptFixtures.receipt(), null);
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
    private record StubSnapshot(long snapshotId, Long parentId, Map<String, String> summary)
            implements Snapshot {
        StubSnapshot(long snapshotId, Long parentId) {
            this(snapshotId, parentId, Map.of());
        }

        @Override public long sequenceNumber() { return 0; }
        @Override public long timestampMillis() { return System.currentTimeMillis(); }
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
        @Override public Iterable<Snapshot> snapshots() {
            if (currentSnapshot == null) {
                return List.of();
            }
            if (parentSnapshot == null) {
                return List.of(currentSnapshot);
            }
            return List.of(currentSnapshot, parentSnapshot);
        }
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
