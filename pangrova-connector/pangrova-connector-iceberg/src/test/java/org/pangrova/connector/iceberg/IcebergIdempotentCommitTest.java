package org.pangrova.connector.iceberg;

import org.pangrova.spi.connector.ConnectorSession;
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

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for {@link IdempotentCommitController} — verifies the commit token
 * generation and idempotency detection logic (F-ICEBERG-02).
 *
 * <p>Uses stub implementations (no Hadoop dependency) to test the logic
 * in isolation on any JDK.</p>
 */
class IcebergIdempotentCommitTest {

    // --- Token generation tests ---

    @Test
    void generateToken_returnsQueryIdAndAttemptId() {
        ConnectorSession session = stubSession(Map.of(
                "queryId", "q-12345",
                "attemptId", "2"));

        String token = IdempotentCommitController.generateToken(session);

        assertThat(token)
                .as("Token should combine queryId and attemptId")
                .isEqualTo("q-12345:2");
    }

    @Test
    void generateToken_defaultsAttemptIdToZero() {
        ConnectorSession session = stubSession(Map.of("queryId", "q-99"));

        String token = IdempotentCommitController.generateToken(session);

        assertThat(token)
                .as("Token should default attemptId to 0")
                .isEqualTo("q-99:0");
    }

    @Test
    void generateToken_returnsNullWhenNoQueryId() {
        ConnectorSession session = stubSession(Map.of());

        String token = IdempotentCommitController.generateToken(session);

        assertThat(token)
                .as("Token should be null when queryId is missing")
                .isNull();
    }

    @Test
    void generateToken_returnsNullForEmptyQueryId() {
        ConnectorSession session = stubSession(Map.of("queryId", ""));

        String token = IdempotentCommitController.generateToken(session);

        assertThat(token)
                .as("Token should be null when queryId is empty")
                .isNull();
    }

    @Test
    void generateToken_returnsNullForNullSession() {
        String token = IdempotentCommitController.generateToken(null);

        assertThat(token)
                .as("Token should be null when session is null")
                .isNull();
    }

    // --- Idempotency check tests ---

    @Test
    void isAlreadyCommitted_returnsTrueWhenTokenFoundInCurrentSnapshot() {
        Map<String, String> summary = Map.of(
                IdempotentCommitController.COMMIT_TOKEN_KEY, "q-100:0",
                "operation", "append");
        StubSnapshot snapshot = new StubSnapshot(200L, null, summary);
        StubTable table = new StubTable(snapshot);

        boolean result = IdempotentCommitController.isAlreadyCommitted(table, "q-100:0");

        assertThat(result)
                .as("Should detect already-committed token in current snapshot")
                .isTrue();
    }

    @Test
    void isAlreadyCommitted_returnsTrueWhenTokenFoundInAncestorSnapshot() {
        // Current snapshot has a different token, parent has our token
        Map<String, String> parentSummary = Map.of(
                IdempotentCommitController.COMMIT_TOKEN_KEY, "q-50:0",
                "operation", "append");
        StubSnapshot parentSnapshot = new StubSnapshot(100L, null, parentSummary);

        Map<String, String> currentSummary = Map.of(
                IdempotentCommitController.COMMIT_TOKEN_KEY, "q-60:0",
                "operation", "append");
        StubSnapshot currentSnapshot = new StubSnapshot(200L, 100L, currentSummary);

        StubTable table = new StubTable(currentSnapshot, parentSnapshot);

        boolean result = IdempotentCommitController.isAlreadyCommitted(table, "q-50:0");

        assertThat(result)
                .as("Should find token in ancestor snapshot")
                .isTrue();
    }

    @Test
    void isAlreadyCommitted_returnsFalseWhenTokenNotFound() {
        Map<String, String> summary = Map.of(
                IdempotentCommitController.COMMIT_TOKEN_KEY, "q-other:0",
                "operation", "append");
        StubSnapshot snapshot = new StubSnapshot(100L, null, summary);
        StubTable table = new StubTable(snapshot);

        boolean result = IdempotentCommitController.isAlreadyCommitted(table, "q-999:0");

        assertThat(result)
                .as("Should not find non-existent token")
                .isFalse();
    }

    @Test
    void isAlreadyCommitted_returnsFalseWhenNoSnapshots() {
        StubTable table = new StubTable(null);

        boolean result = IdempotentCommitController.isAlreadyCommitted(table, "q-100:0");

        assertThat(result)
                .as("Should return false for table with no snapshots")
                .isFalse();
    }

    @Test
    void isAlreadyCommitted_returnsFalseForNullToken() {
        StubSnapshot snapshot = new StubSnapshot(100L, null, Map.of());
        StubTable table = new StubTable(snapshot);

        boolean result = IdempotentCommitController.isAlreadyCommitted(table, null);

        assertThat(result)
                .as("Should return false for null token")
                .isFalse();
    }

    @Test
    void isAlreadyCommitted_returnsFalseForEmptyToken() {
        StubSnapshot snapshot = new StubSnapshot(100L, null, Map.of());
        StubTable table = new StubTable(snapshot);

        boolean result = IdempotentCommitController.isAlreadyCommitted(table, "");

        assertThat(result)
                .as("Should return false for empty token")
                .isFalse();
    }

    @Test
    void isAlreadyCommitted_returnsFalseWhenSnapshotHasNoSummary() {
        StubSnapshot snapshot = new StubSnapshot(100L, null, null);
        StubTable table = new StubTable(snapshot);

        boolean result = IdempotentCommitController.isAlreadyCommitted(table, "q-100:0");

        assertThat(result)
                .as("Should return false when snapshot has no summary")
                .isFalse();
    }

    @Test
    void isAlreadyCommitted_returnsFalseOnRefreshError() {
        StubTable table = new StubTable(null) {
            @Override
            public void refresh() {
                throw new RuntimeException("Catalog unreachable");
            }
        };

        boolean result = IdempotentCommitController.isAlreadyCommitted(table, "q-100:0");

        assertThat(result)
                .as("Should return false on error (safe to proceed with commit)")
                .isFalse();
    }

    @Test
    void isAlreadyCommitted_stopsAfterMaxCheckDepth() {
        // Build a chain longer than MAX_SNAPSHOT_CHECK_DEPTH
        int depth = IdempotentCommitController.MAX_SNAPSHOT_CHECK_DEPTH + 5;
        Map<String, String> targetSummary = Map.of(
                IdempotentCommitController.COMMIT_TOKEN_KEY, "q-target:0");

        // The target token is at the root, beyond the check depth
        StubSnapshot[] chain = new StubSnapshot[depth + 1];
        chain[0] = new StubSnapshot(1L, null, targetSummary); // root with target
        for (int i = 1; i <= depth; i++) {
            chain[i] = new StubSnapshot(i + 1L, (long) i, Map.of());
        }

        // Create table with current = last in chain, root = first
        StubTable table = new StubTable(chain[depth], chain) ;

        boolean result = IdempotentCommitController.isAlreadyCommitted(table, "q-target:0");

        assertThat(result)
                .as("Should not find token beyond max check depth")
                .isFalse();
    }

    @Test
    void commitTokenKey_isStable() {
        assertThat(IdempotentCommitController.COMMIT_TOKEN_KEY)
                .as("Token key should be stable across versions")
                .isEqualTo("pangrova.commit-token");
    }

    @Test
    void maxCheckDepth_isReasonable() {
        assertThat(IdempotentCommitController.MAX_SNAPSHOT_CHECK_DEPTH)
                .as("Check depth should be reasonable (not too small or too large)")
                .isBetween(5, 100);
    }

    // --- Helpers ---

    private static ConnectorSession stubSession(Map<String, String> properties) {
        return new ConnectorSession() {
            @Override public String getUser() { return "test"; }
            @Override public Optional<String> getCatalog() { return Optional.of("iceberg_catalog"); }
            @Override public Optional<String> getSchema() { return Optional.empty(); }
            @Override public Map<String, String> getProperties() { return properties; }
        };
    }

    // --- Stub implementations ---

    /**
     * Minimal Snapshot stub with configurable summary for idempotency testing.
     */
    private record StubSnapshot(long snapshotId, Long parentId,
                                Map<String, String> summary) implements Snapshot {
        @Override public long sequenceNumber() { return 0; }
        @Override public long timestampMillis() { return System.currentTimeMillis(); }
        @Override public Integer schemaId() { return 0; }
        @Override public String operation() { return "append"; }
        @Override public String manifestListLocation() { return ""; }
        @Override
        public List<org.apache.iceberg.ManifestFile> allManifests(FileIO io) { return List.of(); }
        @Override
        public List<org.apache.iceberg.ManifestFile> dataManifests(FileIO io) { return List.of(); }
        @Override
        public List<org.apache.iceberg.ManifestFile> deleteManifests(FileIO io) { return List.of(); }
        @Override
        public Iterable<org.apache.iceberg.DataFile> addedDataFiles(FileIO io) { return List.of(); }
        @Override
        public Iterable<org.apache.iceberg.DataFile> removedDataFiles(FileIO io) { return List.of(); }
    }

    /**
     * Minimal Table stub that returns configurable snapshots.
     * Supports multi-snapshot chains for ancestor traversal testing.
     */
    private static class StubTable implements Table {
        private final Snapshot currentSnapshot;
        private final StubSnapshot[] allSnapshots;

        StubTable(Snapshot currentSnapshot) {
            this(currentSnapshot, (StubSnapshot[]) null);
        }

        StubTable(Snapshot currentSnapshot, Snapshot parentSnapshot) {
            this.currentSnapshot = currentSnapshot;
            if (parentSnapshot instanceof StubSnapshot ps) {
                this.allSnapshots = new StubSnapshot[]{ps};
            } else {
                this.allSnapshots = null;
            }
        }

        StubTable(Snapshot currentSnapshot, StubSnapshot[] allSnapshots) {
            this.currentSnapshot = currentSnapshot;
            this.allSnapshots = allSnapshots;
        }

        @Override public void refresh() { /* no-op */ }
        @Override public Snapshot currentSnapshot() { return currentSnapshot; }

        @Override
        public Snapshot snapshot(long snapshotId) {
            if (currentSnapshot != null && currentSnapshot.snapshotId() == snapshotId) {
                return currentSnapshot;
            }
            if (allSnapshots != null) {
                for (StubSnapshot s : allSnapshots) {
                    if (s != null && s.snapshotId() == snapshotId) {
                        return s;
                    }
                }
            }
            return null;
        }

        // --- Required by Table interface but not used by controller ---
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
