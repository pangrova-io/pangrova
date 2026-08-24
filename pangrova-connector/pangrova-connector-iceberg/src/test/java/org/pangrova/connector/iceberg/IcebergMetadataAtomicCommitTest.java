package org.pangrova.connector.iceberg;

import org.pangrova.connector.iceberg.handle.IcebergOutputTableHandle;
import org.pangrova.spi.connector.ConnectorSession;
import org.pangrova.spi.connector.metadata.table.SchemaTableName;
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

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

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
        @Override public String getUser() { return "test"; }
        @Override public Optional<String> getCatalog() { return Optional.of(CATALOG_NAME); }
        @Override public Optional<String> getSchema() { return Optional.empty(); }
        @Override public Map<String, String> getProperties() { return Map.of(); }
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

    /**
     * Builds a single APPEND fragment as a JSON array containing one DataFile,
     * serialized via ContentFileParser for round-trip fidelity.
     */
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
