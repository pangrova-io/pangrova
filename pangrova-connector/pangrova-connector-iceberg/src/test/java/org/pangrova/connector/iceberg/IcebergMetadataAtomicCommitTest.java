package org.pangrova.connector.iceberg;

import org.pangrova.connector.iceberg.exception.CommitNotificationException;

import org.pangrova.connector.iceberg.handle.IcebergOutputTableHandle;
import org.pangrova.connector.iceberg.enums.IcebergOverwriteScope;
import org.pangrova.connector.iceberg.handle.IcebergOutputHandleCodec;
import org.pangrova.connector.iceberg.write.IcebergPageSinkProvider;
import org.pangrova.connector.api.session.ConnectorSession;
import org.pangrova.connector.api.commit.DistributedWriteCommitContext;
import org.pangrova.connector.api.metadata.table.SchemaTableName;
import org.pangrova.persistence.query.api.commit.WriteCommitFingerprint;
import org.pangrova.persistence.file.api.claim.CopyFileClaim;
import org.apache.iceberg.ContentFileParser;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.SupportsNamespaces;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Unit tests for IcebergMetadata multi-fragment atomic commit via finishDistributedWrite.
 * Verifies that multiple APPEND fragments are merged into a single Iceberg snapshot (PFR-7).
 * Uses real HadoopCatalog on local filesystem (no mocks per pangrova-rules §12).
 * Validates: Requirements 2.6, 10.1
 */
class IcebergMetadataAtomicCommitTest {

    @TempDir
    Path warehouse;

    private Catalog catalog;
    private IcebergMetadata metadata;

    private static final String CATALOG_NAME = "atomic_catalog";
    private static final String NAMESPACE = "atomic_db";

    private static final ConnectorSession SESSION = new ConnectorSession() {
        @Override
        public java.util.Optional<org.pangrova.connector.api.query.ConnectorQueryAuthority> getQueryAuthority() {
            return java.util.Optional.empty();
        }

        @Override public String getUser() { return "test"; }
        @Override public Optional<String> getCatalog() { return Optional.of(CATALOG_NAME); }
        @Override public Optional<String> getSchema() { return Optional.empty(); }
        @Override public Map<String, String> getProperties() {
            return Map.of("tenant_id", "iceberg-atomic-test");
        }
    };

    private static final Schema TEST_SCHEMA = new Schema(
            Types.NestedField.required(1, "id", Types.LongType.get()),
            Types.NestedField.optional(2, "name", Types.StringType.get()));

    @BeforeEach
    void setUp() {
        HadoopTestGuard.assumeHadoopCompatible();
        catalog = buildHadoopCatalog();
        createNamespace();
        metadata = new IcebergMetadata(catalog, CATALOG_NAME);
    }

    @Test
    void requiredPublicationReturnsActualSnapshotAndManifestBytes() throws Exception {
        Table table = createTable("required_publication");
        var handle = beginInsert("required_publication");
        var fragments = List.of(buildAppendFragmentBytes(table, "actual.parquet", 3, 1024));
        var context = context("required-write", handle, fragments);
        var receipt = metadata.finishDistributedWriteWithPublication(SESSION, handle, fragments, context);
        table.refresh();
        var snapshot = table.currentSnapshot();
        assertThat(receipt.publishedVersion()).isEqualTo(snapshot.snapshotId());
        assertThat(snapshot.summary()).containsEntry(IcebergDistributedCommitMarker.FINGERPRINT,
                context.commitFingerprint());
        try (var input = table.io().newInputFile(snapshot.manifestListLocation()).newStream()) {
            assertThat(receipt.manifestSha256()).isEqualTo(
                    java.security.MessageDigest.getInstance("SHA-256").digest(input.readAllBytes()));
        }
        assertThat(metadata.finishDistributedWriteWithPublication(SESSION, handle, fragments, context))
                .isEqualTo(receipt);
        table.refresh();
        assertThat(countSnapshots(table)).isEqualTo(1);
    }

    @ParameterizedTest
    @ValueSource(ints = {0, 1, 32})
    void requiredEmptyInsertCommitsOneRealSnapshotAndReplaysExactly(int fragmentCount) {
        Table table = createTable("required_empty");
        var handle = beginInsert("required_empty");
        var fragments = java.util.Collections.nCopies(fragmentCount, "[]".getBytes(StandardCharsets.UTF_8));
        var context = context("required-empty", handle, fragments);
        assertThat(table.currentSnapshot()).isNull();
        var receipt = metadata.finishDistributedWriteWithPublication(SESSION, handle, fragments, context);
        table.refresh();
        assertThat(table.currentSnapshot().snapshotId()).isEqualTo(receipt.publishedVersion());
        assertThat(table.currentSnapshot().summary()).containsEntry(
                IcebergDistributedCommitMarker.EXECUTION_ID, "required-empty");
        try (var files = table.newScan().planFiles()) {
            assertThat(files).isEmpty();
        } catch (java.io.IOException failure) {
            throw new java.io.UncheckedIOException(failure);
        }
        assertThat(metadata.finishDistributedWriteWithPublication(SESSION, handle, fragments, context))
                .isEqualTo(receipt);
        table.refresh();
        assertThat(countSnapshots(table)).isEqualTo(1);
    }

    @Test
    void multipleAppendFragments_produceSingleSnapshot() {
        Table table = createTable("multi_append");
        int snapshotCountBefore = countSnapshots(table);

        IcebergOutputTableHandle handle = beginInsert("multi_append");

        byte[] fragment1 = buildAppendFragmentBytes(table, "data-001.parquet", 10, 1024);
        byte[] fragment2 = buildAppendFragmentBytes(table, "data-002.parquet", 20, 2048);
        byte[] fragment3 = buildAppendFragmentBytes(table, "data-003.parquet", 30, 3072);

        metadata.finishDistributedWrite(SESSION, handle, List.of(fragment1, fragment2, fragment3));

        table.refresh();
        int snapshotCountAfter = countSnapshots(table);
        assertThat(snapshotCountAfter - snapshotCountBefore)
                .as("Three APPEND fragments should produce exactly one new snapshot")
                .isEqualTo(1);
    }

    @Test
    void multipleAppendFragments_allDataFilesPresent() {
        Table table = createTable("all_files");
        IcebergOutputTableHandle handle = beginInsert("all_files");

        byte[] fragment1 = buildAppendFragmentBytes(table, "file-a.parquet", 5, 500);
        byte[] fragment2 = buildAppendFragmentBytes(table, "file-b.parquet", 15, 1500);
        byte[] fragment3 = buildAppendFragmentBytes(table, "file-c.parquet", 25, 2500);

        metadata.finishDistributedWrite(SESSION, handle, List.of(fragment1, fragment2, fragment3));

        table.refresh();
        Snapshot snapshot = table.currentSnapshot();
        assertThat(snapshot).isNotNull();

        long totalRecords = snapshot.summary().get("total-records") != null
                ? Long.parseLong(snapshot.summary().get("total-records"))
                : -1;
        assertThat(totalRecords)
                .as("All DataFiles from all fragments should be committed")
                .isEqualTo(5 + 15 + 25);
    }

    @Test
    void noFragments_producesNoNewSnapshot() {
        Table table = createTable("empty_commit");
        int snapshotCountBefore = countSnapshots(table);

        IcebergOutputTableHandle handle = beginInsert("empty_commit");

        metadata.finishDistributedWrite(SESSION, handle, List.of());

        table.refresh();
        int snapshotCountAfter = countSnapshots(table);
        assertThat(snapshotCountAfter)
                .as("Empty fragment list should not produce a new snapshot")
                .isEqualTo(snapshotCountBefore);
    }

    @Test
    void recoveryReplayFindsFingerprintInSnapshotLineageAndDoesNotAppendTwice() {
        Table table = createTable("recover_once");
        IcebergOutputTableHandle handle = beginInsert("recover_once");
        byte[] fragment = buildAppendFragmentBytes(table, "recover-once.parquet", 7, 700);
        DistributedWriteCommitContext context = context(
                "q-recover-once", handle, List.of(fragment));

        metadata.finishDistributedWrite(SESSION, handle, List.of(fragment), context);
        table.refresh();
        Snapshot committed = table.currentSnapshot();
        assertThat(committed.summary())
                .containsEntry(IcebergDistributedCommitMarker.FINGERPRINT,
                        context.commitFingerprint())
                .containsEntry(IcebergDistributedCommitMarker.TENANT_ID, context.tenantId())
                .containsEntry(IcebergDistributedCommitMarker.BASE_SNAPSHOT_ID,
                        String.valueOf(handle.baseSnapshotId()));

        for (int later = 0; later < 25; later++) {
            DataFile unrelated = DataFiles.builder(table.spec())
                    .withPath(table.location() + "/data/unrelated-" + later + ".parquet")
                    .withFormat(FileFormat.PARQUET)
                    .withRecordCount(1)
                    .withFileSizeInBytes(100)
                    .build();
            table.newAppend().appendFile(unrelated).commit();
        }
        int snapshotsBeforeReplay = countSnapshots(table);
        long latestSnapshot = table.currentSnapshot().snapshotId();

        IcebergOutputTableHandle rebuilt = (IcebergOutputTableHandle)
                metadata.reconstructOutputHandle(
                        SESSION, new SchemaTableName(NAMESPACE, "recover_once"),
                        "INSERT", handle.baseSnapshotId());
        metadata.finishDistributedWrite(SESSION, rebuilt, List.of(fragment), context);

        table.refresh();
        assertThat(rebuilt.baseSnapshotId()).isEqualTo(handle.baseSnapshotId());
        assertThat(countSnapshots(table)).isEqualTo(snapshotsBeforeReplay);
        assertThat(table.currentSnapshot().snapshotId()).isEqualTo(latestSnapshot);
    }

    @Test
    void replayRejectsMatchingFingerprintWithDifferentTenant() {
        Table table = createTable("inconsistent_marker");
        var handle = beginInsert("inconsistent_marker");
        var fragments = List.of(buildAppendFragmentBytes(table, "staged.parquet", 3, 1024));
        var context = context("inconsistent-marker", handle, fragments);
        var append = table.newAppend();
        IcebergDistributedCommitMarker.apply(append, context);
        append.set(IcebergDistributedCommitMarker.TENANT_ID, "foreign-tenant").commit();

        assertThatThrownBy(() -> metadata.finishDistributedWrite(SESSION, handle, fragments, context))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining(IcebergDistributedCommitMarker.TENANT_ID);
        table.refresh();
        assertThat(countSnapshots(table)).isOne();
    }

    @Test
    void replayRejectsMultipleSnapshotsWithTheSameFingerprint() {
        Table table = createTable("duplicate_marker");
        var handle = beginInsert("duplicate_marker");
        var fragments = List.of(buildAppendFragmentBytes(table, "first.parquet", 3, 1024));
        var context = context("duplicate-marker", handle, fragments);
        metadata.finishDistributedWrite(SESSION, handle, fragments, context);
        table.refresh();
        var append = table.newAppend();
        IcebergDistributedCommitMarker.apply(append, context);
        append.commit();

        assertThatThrownBy(() -> metadata.finishDistributedWrite(SESSION, handle, fragments, context))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("appears in multiple snapshots");
        table.refresh();
        assertThat(countSnapshots(table)).isEqualTo(2);
    }

    @Test
    void replayAfterNotificationFailureUsesOriginalSnapshot() {
        Table table = createTable("replay_event");
        IcebergOutputTableHandle handle = beginInsert("replay_event");
        byte[] fragment = buildAppendFragmentBytes(table, "event-file.parquet", 7, 700);
        DistributedWriteCommitContext context = context(
                "q-replay-event", handle, List.of(fragment));
        AtomicInteger deliveries = new AtomicInteger();
        AtomicReference<org.pangrova.connector.api.index.CommitEvent> replayed =
                new AtomicReference<>();
        metadata.registerCommitListener(event -> {
            if (deliveries.incrementAndGet() == 1) {
                throw new IllegalStateException("commit log unavailable");
            }
            replayed.set(event);
        });

        assertThatThrownBy(() -> metadata.finishDistributedWrite(
                SESSION, handle, List.of(fragment), context))
                .isInstanceOf(CommitNotificationException.class)
                .hasCauseInstanceOf(IllegalStateException.class);
        table.refresh();
        long committedSnapshot = table.currentSnapshot().snapshotId();
        int snapshotsAfterCommit = countSnapshots(table);

        table.newAppend().appendFile(DataFiles.builder(table.spec())
                .withPath(table.location() + "/data/later.parquet")
                .withFormat(FileFormat.PARQUET)
                .withRecordCount(1)
                .withFileSizeInBytes(100)
                .build()).commit();
        metadata.finishDistributedWrite(SESSION, handle, List.of(fragment), context);

        assertThat(countSnapshots(table)).isEqualTo(snapshotsAfterCommit + 1);
        assertThat(replayed.get()).isNotNull();
        assertThat(replayed.get().snapshotId()).isEqualTo(String.valueOf(committedSnapshot));
        assertThat(replayed.get().commitFingerprint()).isEqualTo(context.commitFingerprint());
    }

    @Test
    void replayFailsClosedWhenCommittedMarkerSnapshotWasExpired() {
        Table table = createTable("expired_marker");
        table.newAppend().appendFile(DataFiles.builder(table.spec())
                .withPath(table.location() + "/data/base.parquet")
                .withFormat(FileFormat.PARQUET)
                .withRecordCount(1)
                .withFileSizeInBytes(100)
                .build()).commit();
        IcebergOutputTableHandle handle = beginInsert("expired_marker");
        byte[] fragment = buildAppendFragmentBytes(table, "journaled.parquet", 7, 700);
        DistributedWriteCommitContext context = context(
                "q-expired-marker", handle, List.of(fragment));

        metadata.finishDistributedWrite(SESSION, handle, List.of(fragment), context);
        table.refresh();
        long committedSnapshot = table.currentSnapshot().snapshotId();
        table.newAppend().appendFile(DataFiles.builder(table.spec())
                .withPath(table.location() + "/data/later.parquet")
                .withFormat(FileFormat.PARQUET)
                .withRecordCount(1)
                .withFileSizeInBytes(100)
                .build()).commit();
        table.expireSnapshots().expireSnapshotId(committedSnapshot)
                .cleanExpiredFiles(false).commit();
        int snapshotsBeforeReplay = countSnapshots(table);

        assertThatThrownBy(() -> metadata.finishDistributedWrite(
                SESSION, handle, List.of(fragment), context))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("snapshot ancestry was expired");
        assertThat(countSnapshots(table)).isEqualTo(snapshotsBeforeReplay);
    }

    @Test
    void replayRejectsMarkerOutsideCurrentSnapshotAncestry() {
        Table table = createTable("rolled_back_marker");
        table.newAppend().appendFile(DataFiles.builder(table.spec())
                .withPath(table.location() + "/data/base.parquet")
                .withFormat(FileFormat.PARQUET)
                .withRecordCount(1)
                .withFileSizeInBytes(100)
                .build()).commit();
        table.refresh();
        long baseSnapshot = table.currentSnapshot().snapshotId();
        IcebergOutputTableHandle handle = beginInsert("rolled_back_marker");
        byte[] fragment = buildAppendFragmentBytes(table, "rolled-back.parquet", 7, 700);
        DistributedWriteCommitContext context = context(
                "q-rolled-back-marker", handle, List.of(fragment));

        metadata.finishDistributedWrite(SESSION, handle, List.of(fragment), context);
        table.refresh();
        long publishedSnapshot = table.currentSnapshot().snapshotId();
        table.manageSnapshots().rollbackTo(baseSnapshot).commit();
        table.refresh();
        assertThat(table.snapshot(publishedSnapshot)).isNotNull();

        assertThatThrownBy(() -> metadata.finishDistributedWrite(
                SESSION, handle, List.of(fragment), context))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("not in the current snapshot ancestry");
        assertThat(table.currentSnapshot().snapshotId()).isEqualTo(baseSnapshot);
    }

    @Test
    void copyPublicationProofRequiresExactMarkerInCurrentAncestry() {
        Table table = createTable("copy_marker_ancestry");
        table.newAppend().appendFile(DataFiles.builder(table.spec())
                .withPath(table.location() + "/data/base.parquet")
                .withFormat(FileFormat.PARQUET).withRecordCount(1)
                .withFileSizeInBytes(100).build()).commit();
        table.refresh();
        long baseSnapshot = table.currentSnapshot().snapshotId();
        IcebergOutputTableHandle handle = beginInsert("copy_marker_ancestry");
        byte[] fragment = buildAppendFragmentBytes(table, "copy-file.parquet", 7, 700);
        CopyFileClaim claim = new CopyFileClaim("iceberg-atomic-test", handle.tableUuid(),
                "s3://bucket/data/copy-file.parquet", "version-1", "a".repeat(64), 700L,
                "profile", "00000000-0000-0000-0000-000000000001", 1L, 0L, "q-copy-marker");
        DistributedWriteCommitContext context = copyContext(
                "q-copy-marker", handle, List.of(fragment), claim);

        metadata.finishDistributedWrite(SESSION, handle, List.of(fragment), context);
        table.refresh();
        long published = table.currentSnapshot().snapshotId();
        assertThat(metadata.confirmCopyFilePublication(SESSION, handle, context).snapshotId())
                .isEqualTo(published);
        assertThat(table.currentSnapshot().summary())
                .containsEntry(IcebergDistributedCommitMarker.COPY_FILE_KEY, claim.claimKey())
                .containsEntry(IcebergDistributedCommitMarker.COPY_CLAIM_DIGEST, claim.claimDigest())
                .containsEntry(IcebergDistributedCommitMarker.COPY_TABLE_UUID, claim.tableUuid());

        table.manageSnapshots().rollbackTo(baseSnapshot).commit();
        assertThatThrownBy(() -> metadata.confirmCopyFilePublication(SESSION, handle, context))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("not in the current snapshot ancestry");
    }

    @Test
    void oldJournalCannotWriteRecreatedTableWithSameName() {
        Table original = createTable("recreated_table");
        IcebergOutputTableHandle handle = beginInsert("recreated_table");
        byte[] fragment = buildAppendFragmentBytes(original, "old-write.parquet", 7, 700);
        DistributedWriteCommitContext context = context(
                "q-old-incarnation", handle, List.of(fragment));
        TableIdentifier identifier = TableIdentifier.of(NAMESPACE, "recreated_table");
        assertThat(catalog.dropTable(identifier, true)).isTrue();
        Table recreated = createTable("recreated_table");
        assertThat(recreated.uuid().toString()).isNotEqualTo(handle.tableUuid());

        assertThatThrownBy(() -> metadata.finishDistributedWrite(
                SESSION, handle, List.of(fragment), context))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("table UUID changed");
        assertThatThrownBy(() -> new IcebergPageSinkProvider(catalog)
                .createPageSink(null, SESSION, handle))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("table UUID changed");
        recreated.refresh();
        assertThat(recreated.currentSnapshot()).isNull();
    }

    @Test
    void outputHandleCodecRequiresTableUuid() {
        Table table = createTable("handle_uuid");
        IcebergOutputTableHandle handle = beginInsert("handle_uuid");
        IcebergOutputHandleCodec codec = new IcebergOutputHandleCodec();
        byte[] payload = codec.serialize(handle);

        assertThat(codec.deserialize(codec.codecVersion(), payload)).isEqualTo(handle);
        assertThat(handle.tableUuid()).isEqualTo(table.uuid().toString());
        assertThatThrownBy(() -> codec.deserialize(1, payload))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Unsupported Iceberg output handle version");
    }

    @Test
    void emptyOverwriteRecoveryPublishesTruncateOnlyOnce() {
        Table table = createTable("recover_empty_overwrite");
        table.newAppend().appendFile(DataFiles.builder(table.spec())
                .withPath(table.location() + "/data/original.parquet")
                .withFormat(FileFormat.PARQUET)
                .withRecordCount(3)
                .withFileSizeInBytes(300)
                .build()).commit();
        var tableHandle = metadata.getTableHandle(
                SESSION, new SchemaTableName(NAMESPACE, "recover_empty_overwrite"));
        IcebergOutputTableHandle handle = (IcebergOutputTableHandle)
                metadata.beginOverwrite(SESSION, tableHandle);
        DistributedWriteCommitContext context = context(
                "q-empty-overwrite", handle, List.of());
        int before = countSnapshots(table);

        metadata.finishDistributedWrite(SESSION, handle, List.of(), context);
        metadata.finishDistributedWrite(SESSION, handle, List.of(), context);

        assertThat(countSnapshots(table)).isEqualTo(before + 1);
        table.refresh();
        assertThat(table.currentSnapshot().summary().get("total-records")).isEqualTo("0");
    }

    // --- helpers ---

    @Test
    void abortDistributedWrite_deletesStagedFilesAndCreatesNoSnapshot() throws Exception {
        Table table = createTable("staged_abort");
        int snapshotsBefore = countSnapshots(table);
        IcebergOutputTableHandle handle = beginInsert("staged_abort");

        // Stage a real data file on the table's FileIO (written to storage, but never published).
        String stagedPath = table.location() + "/data/staged-abort-0.parquet";
        try (var out = table.io().newOutputFile(stagedPath).createOrOverwrite()) {
            out.write("staged-but-unpublished".getBytes(StandardCharsets.UTF_8));
        }
        assertThat(table.io().newInputFile(stagedPath).exists())
                .as("staged file should exist before abort").isTrue();

        byte[] fragment = buildAppendFragmentBytes(table, "staged-abort-0.parquet", 5, 1024);
        metadata.abortDistributedWrite(SESSION, handle, List.of(fragment));

        // A-EOS-02: abort deletes the staged file and publishes NO snapshot (true 2PC discard).
        assertThat(table.io().newInputFile(stagedPath).exists())
                .as("aborted staged file must be deleted").isFalse();
        assertThat(countSnapshots(table))
                .as("abort must not create a snapshot").isEqualTo(snapshotsBefore);
    }

    @Test
    void reconstructOutputHandle_rebuildsHandleUsableByFinishDistributedWrite() {
        Table table = createTable("reconstruct_me");
        // A-COMMIT-01: a recovering coordinator rebuilds the handle from the live table (no original).
        IcebergOutputTableHandle rebuilt = (IcebergOutputTableHandle) metadata.reconstructOutputHandle(
                SESSION, new SchemaTableName(NAMESPACE, "reconstruct_me"), "INSERT");
        assertThat(rebuilt.schemaName()).isEqualTo(NAMESPACE);
        assertThat(rebuilt.tableName()).isEqualTo("reconstruct_me");
        assertThat(rebuilt.writeMode()).isEqualTo(IcebergOutputTableHandle.MODE_INSERT);
        assertThat(rebuilt.targetTable()).contains(new SchemaTableName(NAMESPACE, "reconstruct_me"));

        // The rebuilt handle must be accepted by finishDistributedWrite (the recovery re-drive path):
        // committing a worker fragment through it lands exactly one new snapshot.
        int before = countSnapshots(table);
        byte[] fragment = buildAppendFragmentBytes(table, "reconstructed-0.parquet", 3, 512);
        metadata.finishDistributedWrite(SESSION, rebuilt, List.of(fragment));
        assertThat(countSnapshots(table))
                .as("rebuilt handle drives a real commit").isEqualTo(before + 1);
    }

    @Test
    void reconstructOutputHandleRejectsUnknownWriteMode() {
        createTable("reject_unknown_mode");
        SchemaTableName table = new SchemaTableName(NAMESPACE, "reject_unknown_mode");
        for (String mode : List.of("UNKNOWN", "APPEND", "overwrite", "")) {
            assertThatThrownBy(() -> metadata.reconstructOutputHandle(SESSION, table, mode))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("write recovery mode");
        }
        assertThatThrownBy(() -> metadata.reconstructOutputHandle(SESSION, table, null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("write recovery mode");
    }

    @Test
    void outputHandleRejectsUnknownWriteMode() {
        assertThatThrownBy(() -> new IcebergOutputTableHandle(
                NAMESPACE, "orders", "00000000-0000-0000-0000-000000000001",
                "file:///warehouse/orders", "{}", "{}",
                2, IcebergOutputTableHandle.NO_SNAPSHOT, null, IcebergOverwriteScope.NONE,
                Map.of(), Map.of()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Unsupported Iceberg write mode");
    }

    private Catalog buildHadoopCatalog() {
        Map<String, String> props = Map.of(
                "type", "hadoop",
                "warehouse", warehouse.toString());
        return org.apache.iceberg.CatalogUtil.buildIcebergCatalog(
                CATALOG_NAME, props, new org.apache.hadoop.conf.Configuration());
    }

    private void createNamespace() {
        if (catalog instanceof SupportsNamespaces nsCatalog) {
            nsCatalog.createNamespace(Namespace.of(NAMESPACE));
        }
    }

    private Table createTable(String tableName) {
        TableIdentifier tableId = TableIdentifier.of(NAMESPACE, tableName);
        return catalog.createTable(tableId, TEST_SCHEMA, PartitionSpec.unpartitioned());
    }

    private IcebergOutputTableHandle beginInsert(String tableName) {
        var tableHandle = metadata.getTableHandle(
                SESSION, new SchemaTableName(NAMESPACE, tableName));
        return (IcebergOutputTableHandle) metadata.beginInsert(SESSION, tableHandle);
    }

    private DistributedWriteCommitContext context(
            String executionId,
            IcebergOutputTableHandle handle,
            List<byte[]> fragments) {
        String tenantId = SESSION.getProperties().get("tenant_id");
        byte[] binding = IcebergWriteBindingFixtures.binding(handle);
        String fingerprint = WriteCommitFingerprint.compute(
                new org.pangrova.persistence.query.api.commit.WriteCommitFingerprint.Intent(
                        executionId,
                        tenantId,
                        CATALOG_NAME,
                        new SchemaTableName(handle.schemaName(), handle.tableName()).toString(),
                        handle.writeMode(),
                        handle.baseSnapshotId(),
                        binding,
                        SESSION.getUser(),
                        IcebergWriteReceiptFixtures.receipt(), null),
                fragments);
        return new DistributedWriteCommitContext(
                executionId,
                tenantId,
                handle.baseSnapshotId(),
                binding,
                fingerprint,
                SESSION.getUser(),
                IcebergWriteReceiptFixtures.receipt(), null);
    }

    /**
     * Builds a single APPEND fragment as a JSON array containing one DataFile,
     * serialized via ContentFileParser for round-trip fidelity.
     */
    private DistributedWriteCommitContext copyContext(String executionId,
            IcebergOutputTableHandle handle, List<byte[]> fragments, CopyFileClaim claim) {
        String tenantId = SESSION.getProperties().get("tenant_id");
        byte[] binding = IcebergWriteBindingFixtures.binding(handle);
        String fingerprint = WriteCommitFingerprint.compute(
                new WriteCommitFingerprint.Intent(executionId, tenantId, CATALOG_NAME,
                        new SchemaTableName(handle.schemaName(), handle.tableName()).toString(),
                        handle.writeMode(), handle.baseSnapshotId(), binding, SESSION.getUser(),
                        IcebergWriteReceiptFixtures.receipt(), claim.commitBinding()), fragments);
        return new DistributedWriteCommitContext(executionId, tenantId, handle.baseSnapshotId(),
                binding, fingerprint, SESSION.getUser(), IcebergWriteReceiptFixtures.receipt(),
                claim.commitBinding());
    }

    private byte[] buildAppendFragmentBytes(Table table, String fileName,
                                            long recordCount, long fileSizeInBytes) {
        DataFile dataFile = DataFiles.builder(table.spec())
                .withPath(table.location() + "/data/" + fileName)
                .withFormat(FileFormat.PARQUET)
                .withRecordCount(recordCount)
                .withFileSizeInBytes(fileSizeInBytes)
                .build();
        String dataFileJson = ContentFileParser.toJson(dataFile, table.spec());
        String arrayJson = "[" + dataFileJson + "]";
        return arrayJson.getBytes(StandardCharsets.UTF_8);
    }

    private int countSnapshots(Table table) {
        table.refresh();
        int count = 0;
        for (Snapshot ignored : table.snapshots()) {
            count++;
        }
        return count;
    }
}
