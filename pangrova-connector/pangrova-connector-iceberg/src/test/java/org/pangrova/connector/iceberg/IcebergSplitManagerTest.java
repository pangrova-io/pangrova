package org.pangrova.connector.iceberg;

import org.pangrova.connector.iceberg.handle.IcebergSplit;
import org.pangrova.connector.iceberg.handle.IcebergTableHandle;
import org.pangrova.connector.iceberg.handle.IcebergTableHandleFactory;
import org.pangrova.connector.iceberg.read.IcebergExpressionToSubstrait;
import org.pangrova.spi.connector.ConnectorSession;
import org.pangrova.spi.connector.ConnectorTransactionHandle;
import org.pangrova.spi.connector.metadata.ConnectorSplit;
import org.pangrova.spi.connector.metadata.ConnectorSplitSource;
import org.pangrova.spi.connector.metadata.Constraint;
import org.pangrova.spi.connector.metadata.DynamicFilter;
import org.pangrova.protocol.DataSplitProtoCodec;
import org.pangrova.protocol.v2.DataSplit;
import org.pangrova.protocol.v2.IcebergReadMode;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.SupportsNamespaces;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration test for IcebergSplitManager using real HadoopCatalog.
 * Validates: Requirement 30.6 — getSplits returns one IcebergSplit per FileScanTask.
 */
class IcebergSplitManagerTest {

    @Test
    void nullOnlyColumnOverridesPhysicalColumnEvenAfterRename() {
        Schema schema = new Schema(
                Types.NestedField.required(1, "id", Types.LongType.get()),
                Types.NestedField.optional(2, "future", Types.UnknownType.get()));
        var mappings = IcebergTaskToSplit.withNullOnlyColumns(schema,
                List.of(new org.pangrova.spi.connector.metadata.table.ColumnMapping(
                        2, 2, "old_future", "future")));
        assertThat(mappings).hasSize(2);
        assertThat(mappings.get(1).sourceFieldId()).isEqualTo(-1);
        assertThat(mappings.get(1).targetName()).isEqualTo("future");
    }

    @TempDir
    Path warehouse;

    private Catalog catalog;
    private IcebergSplitManager splitManager;

    private static final ConnectorTransactionHandle TX = new ConnectorTransactionHandle() {};
    private static final DynamicFilter NO_FILTER = DynamicFilter.ALL;
    private static final Constraint NO_CONSTRAINT = Constraint.EMPTY;

    private static final ConnectorSession SESSION = new ConnectorSession() {
        @Override
        public java.util.Optional<org.pangrova.spi.connector.ConnectorQueryAuthority> getQueryAuthority() {
            return java.util.Optional.empty();
        }

        @Override public String getUser() { return "test"; }
        @Override public Optional<String> getCatalog() { return Optional.of("test_catalog"); }
        @Override public Optional<String> getSchema() { return Optional.empty(); }
        @Override public Map<String, String> getProperties() { return Map.of(); }
    };

    private static final Schema TEST_SCHEMA = new Schema(
            Types.NestedField.required(1, "id", Types.LongType.get()),
            Types.NestedField.optional(2, "name", Types.StringType.get()));

    @BeforeEach
    void setUp() {
        HadoopTestGuard.assumeHadoopCompatible();
        catalog = createHadoopCatalog();
        splitManager = new IcebergSplitManager("test_catalog", catalog, new IcebergExpressionToSubstrait());
    }

    @Test
    void getSplits_emptyTableReturnsNoSplits() {
        Table table = createTable("db", "empty_table", TEST_SCHEMA);
        IcebergTableHandle handle = handleFor("db", "empty_table", table);

        List<ConnectorSplit> splits = drainSplits(handle);

        assertThat(splits).isEmpty();
    }

    @Test
    void getSplits_returnsOneSplitPerDataFile() {
        Table table = createTable("db", "orders", TEST_SCHEMA);
        appendDataFile(table, "/data/file1.parquet", 1024L);
        appendDataFile(table, "/data/file2.parquet", 2048L);

        IcebergTableHandle handle = handleFor("db", "orders", table);
        List<ConnectorSplit> splits = drainSplits(handle);

        assertThat(splits).hasSize(2);
        assertThat(splits).allSatisfy(s -> assertThat(s).isInstanceOf(IcebergSplit.class));
    }

    @Test
    void getSplits_splitContainsFilePathAndFormat() {
        Table table = createTable("db", "products", TEST_SCHEMA);
        appendDataFile(table, "/data/products-001.parquet", 512L);

        IcebergTableHandle handle = handleFor("db", "products", table);
        List<ConnectorSplit> splits = drainSplits(handle);

        assertThat(splits).hasSize(1);
        IcebergSplit split = (IcebergSplit) splits.get(0);
        assertThat(split.filePath()).endsWith("products-001.parquet");
        assertThat(split.fileFormat()).isEqualTo("PARQUET");
        assertThat(split.fileLength()).isEqualTo(512L);
        assertThat(split.fileOffset()).isEqualTo(0L);
    }

    @Test
    void getSplits_splitContainsEmptyDeleteFiles() {
        Table table = createTable("db", "clean", TEST_SCHEMA);
        appendDataFile(table, "/data/clean-001.parquet", 256L);

        IcebergTableHandle handle = handleFor("db", "clean", table);
        List<ConnectorSplit> splits = drainSplits(handle);

        IcebergSplit split = (IcebergSplit) splits.get(0);
        assertThat(split.deleteFiles()).isEmpty();
    }

    @Test
    void getSplits_resolvesTableFromCatalogWhenHandleHasNoTableRef() {
        Table table = createTable("db", "lazy", TEST_SCHEMA);
        appendDataFile(table, "/data/lazy-001.parquet", 128L);

        // Handle created from factory — SplitManager always loads fresh from catalog
        IcebergTableHandle handle = IcebergTableHandleFactory.fromTable("db", "lazy", table);
        List<ConnectorSplit> splits = drainSplits(handle);

        assertThat(splits).hasSize(1);
    }

    @Test
    void getSplits_splitSourceIsFinishedAfterDrain() {
        Table table = createTable("db", "finite", TEST_SCHEMA);
        appendDataFile(table, "/data/finite-001.parquet", 64L);

        IcebergTableHandle handle = handleFor("db", "finite", table);
        ConnectorSplitSource source = splitManager.getSplits(TX, SESSION, handle, NO_FILTER, NO_CONSTRAINT);

        assertThat(source.isFinished()).isFalse();
        source.getNextBatch(100);
        assertThat(source.isFinished()).isTrue();
        source.close();
    }

    // -- partition tests (R4.2, R4.3) --

    @Test
    void getSplits_identityPartitionedTableIncludesPartitionValues() {
        Schema schema = new Schema(
                Types.NestedField.required(1, "id", Types.LongType.get()),
                Types.NestedField.optional(2, "category", Types.StringType.get()));
        PartitionSpec spec = PartitionSpec.builderFor(schema)
                .identity("category")
                .build();
        Table table = createPartitionedTable("db", "identity_part", schema, spec);

        DataFile dataFile = DataFiles.builder(spec)
                .withPath("/data/category=electronics/file1.parquet")
                .withFileSizeInBytes(1024L)
                .withRecordCount(10)
                .withFormat(FileFormat.PARQUET)
                .withPartitionPath("category=electronics")
                .build();
        table.newAppend().appendFile(dataFile).commit();

        IcebergTableHandle handle = handleFor("db", "identity_part", table);
        List<ConnectorSplit> splits = drainSplits(handle);

        assertThat(splits).hasSize(1);
        IcebergSplit split = (IcebergSplit) splits.get(0);
        assertThat(split.partitionValues()).containsEntry("category", "electronics");
    }

    @Test
    void getSplits_yearTransformPartitionEvaluatesValue() {
        Schema schema = new Schema(
                Types.NestedField.required(1, "id", Types.LongType.get()),
                Types.NestedField.optional(2, "ts", Types.TimestampType.withoutZone()));
        PartitionSpec spec = PartitionSpec.builderFor(schema)
                .year("ts")
                .build();
        Table table = createPartitionedTable("db", "year_part", schema, spec);

        // Year 54 = 2024 (Iceberg epoch is 1970)
        DataFile dataFile = DataFiles.builder(spec)
                .withPath("/data/ts_year=54/file1.parquet")
                .withFileSizeInBytes(512L)
                .withRecordCount(5)
                .withFormat(FileFormat.PARQUET)
                .withPartitionPath("ts_year=54")
                .build();
        table.newAppend().appendFile(dataFile).commit();

        IcebergTableHandle handle = handleFor("db", "year_part", table);
        List<ConnectorSplit> splits = drainSplits(handle);

        assertThat(splits).hasSize(1);
        IcebergSplit split = (IcebergSplit) splits.get(0);
        // Hidden transform: field name is "ts_year", value is the evaluated year integer
        assertThat(split.partitionValues()).containsKey("ts_year");
        assertThat(split.partitionValues().get("ts_year")).isEqualTo("54");
    }

    @Test
    void getSplits_bucketTransformPartitionEvaluatesValue() {
        Schema schema = new Schema(
                Types.NestedField.required(1, "id", Types.LongType.get()),
                Types.NestedField.optional(2, "name", Types.StringType.get()));
        PartitionSpec spec = PartitionSpec.builderFor(schema)
                .bucket("id", 16)
                .build();
        Table table = createPartitionedTable("db", "bucket_part", schema, spec);

        DataFile dataFile = DataFiles.builder(spec)
                .withPath("/data/id_bucket=5/file1.parquet")
                .withFileSizeInBytes(256L)
                .withRecordCount(3)
                .withFormat(FileFormat.PARQUET)
                .withPartitionPath("id_bucket=5")
                .build();
        table.newAppend().appendFile(dataFile).commit();

        IcebergTableHandle handle = handleFor("db", "bucket_part", table);
        List<ConnectorSplit> splits = drainSplits(handle);

        assertThat(splits).hasSize(1);
        IcebergSplit split = (IcebergSplit) splits.get(0);
        assertThat(split.partitionValues()).containsKey("id_bucket");
        assertThat(split.partitionValues().get("id_bucket")).isEqualTo("5");
    }

    @Test
    void getSplits_partitionValuesSerializedToProto() throws Exception {
        Schema schema = new Schema(
                Types.NestedField.required(1, "id", Types.LongType.get()),
                Types.NestedField.optional(2, "region", Types.StringType.get()));
        PartitionSpec spec = PartitionSpec.builderFor(schema)
                .identity("region")
                .build();
        Table table = createPartitionedTable("db", "proto_part", schema, spec);

        DataFile dataFile = DataFiles.builder(spec)
                .withPath("/data/region=us-east/file1.parquet")
                .withFileSizeInBytes(1024L)
                .withRecordCount(10)
                .withFormat(FileFormat.PARQUET)
                .withPartitionPath("region=us-east")
                .build();
        table.newAppend().appendFile(dataFile).commit();

        IcebergTableHandle handle = handleFor("db", "proto_part", table);
        List<ConnectorSplit> splits = drainSplits(handle);

        IcebergSplit split = (IcebergSplit) splits.get(0);
        byte[] protoBytes = split.toSplitInfoProto();
        DataSplit info = DataSplit.parseFrom(protoBytes);

        assertThat(DataSplitProtoCodec.stringPartitionValues(info))
                .containsEntry("region", "us-east");
    }

    // -- incremental + time-travel tests (R7.1, R7.2, R7.3, R7.4, R7.5) --

    /**
     * Validates: Requirements R7.1, R7.5
     * Incremental read produces only data files added between two snapshots.
     */
    @Test
    void getSplits_incrementalReadReturnsOnlyNewFiles() {
        Table table = createTable("db", "incr_table", TEST_SCHEMA);
        appendDataFile(table, "/data/batch1.parquet", 1024L);
        long snapshot1 = table.currentSnapshot().snapshotId();

        appendDataFile(table, "/data/batch2.parquet", 2048L);
        long snapshot2 = table.currentSnapshot().snapshotId();

        IcebergTableHandle handle = handleFor("db", "incr_table", table)
                .withIncrementalRead(snapshot1, snapshot2);
        List<ConnectorSplit> splits = drainSplits(handle);

        // Only the file added in the second commit should appear
        assertThat(splits).hasSize(1);
        IcebergSplit split = (IcebergSplit) splits.get(0);
        assertThat(split.filePath()).endsWith("batch2.parquet");
        assertThat(split.readMode()).isEqualTo("INCREMENTAL");
        assertThat(split.fromSnapshotId()).isEqualTo(snapshot1);
        assertThat(split.toSnapshotId()).isEqualTo(snapshot2);
    }

    /**
     * Validates: Requirements R7.1
     * Incremental read across multiple commits returns all new files.
     */
    @Test
    void getSplits_incrementalReadAcrossMultipleCommits() {
        Table table = createTable("db", "incr_multi", TEST_SCHEMA);
        appendDataFile(table, "/data/initial.parquet", 512L);
        long snapshot1 = table.currentSnapshot().snapshotId();

        appendDataFile(table, "/data/second.parquet", 1024L);
        appendDataFile(table, "/data/third.parquet", 2048L);
        long snapshot3 = table.currentSnapshot().snapshotId();

        IcebergTableHandle handle = handleFor("db", "incr_multi", table)
                .withIncrementalRead(snapshot1, snapshot3);
        List<ConnectorSplit> splits = drainSplits(handle);

        // Files from second and third commits
        assertThat(splits).hasSize(2);
        List<String> paths = splits.stream()
                .map(s -> ((IcebergSplit) s).filePath())
                .toList();
        assertThat(paths).anyMatch(p -> p.endsWith("second.parquet"));
        assertThat(paths).anyMatch(p -> p.endsWith("third.parquet"));
    }

    /**
     * Validates: Requirements R7.4
     * Incremental split serializes read_mode, from_snapshot_id, to_snapshot_id to proto.
     */
    @Test
    void getSplits_incrementalSplitSerializesToProto() throws Exception {
        Table table = createTable("db", "incr_proto", TEST_SCHEMA);
        appendDataFile(table, "/data/base.parquet", 512L);
        long snapshot1 = table.currentSnapshot().snapshotId();

        appendDataFile(table, "/data/delta.parquet", 1024L);
        long snapshot2 = table.currentSnapshot().snapshotId();

        IcebergTableHandle handle = handleFor("db", "incr_proto", table)
                .withIncrementalRead(snapshot1, snapshot2);
        List<ConnectorSplit> splits = drainSplits(handle);

        IcebergSplit split = (IcebergSplit) splits.get(0);
        DataSplit info = DataSplit.parseFrom(split.toSplitInfoProto());

        var detail = DataSplitProtoCodec.fileDetail(info).getIceberg();
        assertThat(detail.getReadMode())
                .isEqualTo(IcebergReadMode.ICEBERG_READ_MODE_INCREMENTAL);
        assertThat(detail.getFromSnapshotId()).isEqualTo(snapshot1);
        assertThat(detail.getToSnapshotId()).isEqualTo(snapshot2);
        assertThat(info.getSourceSnapshot()).isEqualTo(Long.toString(snapshot2));
        assertThat(info.getSourceId()).isEqualTo("test_catalog.db.incr_proto");
        assertThat(detail.getSnapshotId()).isEqualTo(snapshot2);
    }

    /**
     * Validates: Requirements R7.3
     * Time-travel query reads at a specific historical snapshot.
     */
    @Test
    void getSplits_timeTravelReadsAtSpecificSnapshot() {
        Table table = createTable("db", "tt_table", TEST_SCHEMA);
        appendDataFile(table, "/data/v1.parquet", 512L);
        long snapshot1 = table.currentSnapshot().snapshotId();

        appendDataFile(table, "/data/v2.parquet", 1024L);

        // Time-travel to snapshot1 — should only see v1.parquet
        IcebergTableHandle handle = handleFor("db", "tt_table", table)
                .withSnapshotId(snapshot1);
        List<ConnectorSplit> splits = drainSplits(handle);

        assertThat(splits).hasSize(1);
        IcebergSplit split = (IcebergSplit) splits.get(0);
        assertThat(split.filePath()).endsWith("v1.parquet");
        assertThat(split.readMode()).isEqualTo("TIME_TRAVEL");
    }

    /**
     * Validates: Requirements R7.3, R7.4
     * Time-travel selection serializes a physical snapshot scan with its exact historical snapshot.
     */
    @Test
    void getSplits_timeTravelSplitSerializesToProto() throws Exception {
        Table table = createTable("db", "tt_proto", TEST_SCHEMA);
        appendDataFile(table, "/data/snap1.parquet", 512L);
        long snapshot1 = table.currentSnapshot().snapshotId();

        appendDataFile(table, "/data/snap2.parquet", 1024L);

        IcebergTableHandle handle = handleFor("db", "tt_proto", table)
                .withSnapshotId(snapshot1);
        List<ConnectorSplit> splits = drainSplits(handle);

        IcebergSplit split = (IcebergSplit) splits.get(0);
        DataSplit info = DataSplit.parseFrom(split.toSplitInfoProto());

        var detail = DataSplitProtoCodec.fileDetail(info).getIceberg();
        assertThat(detail.getReadMode())
                .isEqualTo(IcebergReadMode.ICEBERG_READ_MODE_SNAPSHOT);
        assertThat(detail.getSnapshotId()).isEqualTo(snapshot1);
        assertThat(info.getSourceSnapshot()).isEqualTo(Long.toString(snapshot1));
        assertThat(info.getSourceId()).isEqualTo("test_catalog.db.tt_proto");
    }

    @Test
    void getSplits_fullSnapshotIdentifiesReadSnapshotForOlderFiles() throws Exception {
        Table table = createTable("db", "full_snapshot_identity", TEST_SCHEMA);
        appendDataFile(table, "/data/old.parquet", 512L);
        long firstSnapshot = table.currentSnapshot().snapshotId();
        appendDataFile(table, "/data/new.parquet", 1024L);
        long readSnapshot = table.currentSnapshot().snapshotId();

        List<ConnectorSplit> splits = drainSplits(handleFor("db", "full_snapshot_identity", table));

        assertThat(readSnapshot).isNotEqualTo(firstSnapshot);
        assertThat(splits).hasSize(2);
        for (ConnectorSplit split : splits) {
            DataSplit proto = DataSplit.parseFrom(split.toSplitInfoProto());
            assertThat(proto.getSourceId())
                    .isEqualTo("test_catalog.db.full_snapshot_identity");
            assertThat(proto.getSourceSnapshot()).isEqualTo(Long.toString(readSnapshot));
            assertThat(DataSplitProtoCodec.fileDetail(proto).getIceberg().getSnapshotId())
                    .isEqualTo(readSnapshot);
        }
    }

    // -- split constraint tests (H.5) --

    @Test
    void getSplits_partitionedSplitCarriesSplitConstraint() throws Exception {
        Schema schema = new Schema(
                Types.NestedField.required(1, "id", Types.LongType.get()),
                Types.NestedField.optional(2, "country", Types.StringType.get()));
        PartitionSpec spec = PartitionSpec.builderFor(schema)
                .identity("country")
                .build();
        Table table = createPartitionedTable("db", "constraint_part", schema, spec);

        DataFile dataFile = DataFiles.builder(spec)
                .withPath("/data/country=US/file1.parquet")
                .withFileSizeInBytes(1024L)
                .withRecordCount(10)
                .withFormat(FileFormat.PARQUET)
                .withPartitionPath("country=US")
                .build();
        table.newAppend().appendFile(dataFile).commit();

        IcebergTableHandle handle = handleFor("db", "constraint_part", table);
        List<ConnectorSplit> splits = drainSplits(handle);

        assertThat(splits).hasSize(1);
        IcebergSplit split = (IcebergSplit) splits.get(0);

        // Verify SPI-level splitConstraint carries partition values
        var constraint = split.splitConstraint();
        assertThat(constraint.hasPredicates()).isTrue();
        assertThat(constraint.partitionValues()).containsEntry("country", "US");

        // Verify proto-level split_constraint carries partition values
        byte[] protoBytes = split.toSplitInfoProto();
        DataSplit info = DataSplit.parseFrom(protoBytes);
        assertThat(DataSplitProtoCodec.stringPartitionValues(info))
                .containsEntry("country", "US");
    }

    @Test
    void getSplits_unpartitionedSplitHasNoConstraint() {
        Table table = createTable("db", "no_constraint", TEST_SCHEMA);
        appendDataFile(table, "/data/no_part.parquet", 512L);

        IcebergTableHandle handle = handleFor("db", "no_constraint", table);
        List<ConnectorSplit> splits = drainSplits(handle);

        assertThat(splits).hasSize(1);
        IcebergSplit split = (IcebergSplit) splits.get(0);

        var constraint = split.splitConstraint();
        assertThat(constraint.hasPredicates()).isFalse();
    }

    // -- helpers --

    private List<ConnectorSplit> drainSplits(IcebergTableHandle handle) {
        ConnectorSplitSource source = splitManager.getSplits(TX, SESSION, handle, NO_FILTER, NO_CONSTRAINT);
        List<ConnectorSplit> splits = source.getNextBatch(1000);
        source.close();
        return splits;
    }

    private IcebergTableHandle handleFor(String schema, String table, Table icebergTable) {
        return IcebergTableHandleFactory.fromTable(schema, table, icebergTable);
    }

    private Table createTable(String namespace, String tableName, Schema schema) {
        createNamespace(namespace);
        TableIdentifier id = TableIdentifier.of(namespace, tableName);
        return catalog.createTable(id, schema);
    }

    private Table createPartitionedTable(String namespace, String tableName, Schema schema, PartitionSpec spec) {
        createNamespace(namespace);
        TableIdentifier id = TableIdentifier.of(namespace, tableName);
        return catalog.createTable(id, schema, spec);
    }

    private void appendDataFile(Table table, String filePath, long fileSizeBytes) {
        DataFile dataFile = DataFiles.builder(PartitionSpec.unpartitioned())
                .withPath(filePath)
                .withFileSizeInBytes(fileSizeBytes)
                .withRecordCount(10)
                .withFormat(FileFormat.PARQUET)
                .build();
        table.newAppend().appendFile(dataFile).commit();
    }

    private Catalog createHadoopCatalog() {
        Map<String, String> props = Map.of(
                "type", "hadoop",
                "warehouse", warehouse.toString());
        org.apache.hadoop.conf.Configuration hadoopConf = new org.apache.hadoop.conf.Configuration();
        return org.apache.iceberg.CatalogUtil.buildIcebergCatalog("test_catalog", props, hadoopConf);
    }

    private void createNamespace(String name) {
        if (catalog instanceof SupportsNamespaces nsCatalog) {
            try {
                nsCatalog.createNamespace(Namespace.of(name));
            } catch (org.apache.iceberg.exceptions.AlreadyExistsException ignored) {
                // namespace already exists
            }
        }
    }
}
