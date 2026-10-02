package org.pangrova.connector.iceberg;

import org.pangrova.connector.iceberg.handle.IcebergColumnHandle;
import org.pangrova.connector.iceberg.handle.IcebergTableHandle;
import org.pangrova.connector.iceberg.handle.IcebergSystemTableHandle;
import org.pangrova.connector.iceberg.handle.IcebergSystemTableSplit;
import org.pangrova.connector.iceberg.read.IcebergExpressionToSubstrait;
import org.pangrova.protocol.DataSplitProtoCodec;
import org.pangrova.protocol.v2.DataSplit;
import org.pangrova.protocol.v2.FileFormat;
import org.pangrova.protocol.v2.SystemTableSplitDetail;
import org.pangrova.spi.connector.ConnectorSession;
import org.pangrova.spi.connector.ConnectorTransactionHandle;
import org.pangrova.spi.connector.data.type.PangrovaType;
import org.pangrova.spi.connector.metadata.table.ColumnHandle;
import org.pangrova.spi.connector.metadata.table.ConnectorTableHandle;
import org.pangrova.spi.connector.metadata.table.ConnectorTableMetadata;
import org.pangrova.spi.connector.metadata.table.SchemaTableName;
import org.pangrova.spi.connector.metadata.Constraint;
import org.pangrova.spi.connector.metadata.DynamicFilter;
import org.pangrova.spi.connector.metadata.SnapshotReference;
import org.pangrova.spi.connector.metadata.ConditionalBranchDropResult;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.HasTableOperations;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.exceptions.CommitStateUnknownException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.lang.reflect.Proxy;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Integration test for IcebergMetadata using real HadoopCatalog on local filesystem.
 * Validates: Requirements 30.1, 30.2, 30.3, 30.4, 30.5.
 */
class IcebergMetadataTest {

    @TempDir
    Path warehouse;

    private Catalog catalog;
    private IcebergMetadata metadata;

    private static final ConnectorSession SESSION = new ConnectorSession() {
        @Override public String getUser() { return "test"; }
        @Override public Optional<String> getCatalog() { return Optional.of("test_catalog"); }
        @Override public Optional<String> getSchema() { return Optional.empty(); }
        @Override public Map<String, String> getProperties() { return Map.of(); }
    };

    private static final Schema TEST_SCHEMA = new Schema(
            Types.NestedField.required(1, "id", Types.LongType.get()),
            Types.NestedField.optional(2, "name", Types.StringType.get()),
            Types.NestedField.optional(3, "amount", Types.DoubleType.get()));

    @BeforeEach
    void setUp() {
        HadoopTestGuard.assumeHadoopCompatible();
        catalog = createHadoopCatalog();
        metadata = new IcebergMetadata(catalog, "test_catalog");
    }

    @Test
    void listSchemaNames_returnsCreatedNamespaces() {
        createNamespace("sales");
        createNamespace("analytics");

        List<String> schemas = metadata.listSchemaNames(SESSION);

        assertThat(schemas).containsExactlyInAnyOrder("sales", "analytics");
    }

    @Test
    void listSchemaNames_emptyWhenNoNamespaces() {
        List<String> schemas = metadata.listSchemaNames(SESSION);
        assertThat(schemas).isEmpty();
    }

    @Test
    void listTables_returnsTablesInSchema() {
        createNamespace("db");
        catalog.createTable(TableIdentifier.of("db", "orders"), TEST_SCHEMA);
        catalog.createTable(TableIdentifier.of("db", "customers"), TEST_SCHEMA);

        List<SchemaTableName> tables = metadata.listTables(SESSION, Optional.of("db"));

        assertThat(tables).containsExactlyInAnyOrder(
                new SchemaTableName("db", "orders"),
                new SchemaTableName("db", "customers"));
    }

    @Test
    void listTables_emptyForNonexistentSchema() {
        List<SchemaTableName> tables = metadata.listTables(SESSION, Optional.of("nonexistent"));
        assertThat(tables).isEmpty();
    }

    @Test
    void getTableHandle_returnsHandleWithMetadataSnapshot() {
        createNamespace("db");
        catalog.createTable(TableIdentifier.of("db", "orders"), TEST_SCHEMA);

        ConnectorTableHandle handle = metadata.getTableHandle(SESSION, new SchemaTableName("db", "orders"));

        assertThat(handle).isNotNull().isInstanceOf(IcebergTableHandle.class);
        IcebergTableHandle icebergHandle = (IcebergTableHandle) handle;
        assertThat(icebergHandle.schemaName()).isEqualTo("db");
        assertThat(icebergHandle.tableName()).isEqualTo("orders");
        assertThat(icebergHandle.tableLocation()).isNotBlank();
        assertThat(icebergHandle.schemaJson()).isNotBlank();
    }

    @Test
    void getTableHandle_returnsNullForNonexistentTable() {
        createNamespace("db");

        ConnectorTableHandle handle = metadata.getTableHandle(SESSION, new SchemaTableName("db", "missing"));

        assertThat(handle).isNull();
    }

    @Test
    void getTableMetadata_returnsTypedColumns() {
        createNamespace("db");
        catalog.createTable(TableIdentifier.of("db", "orders"), TEST_SCHEMA);
        ConnectorTableHandle handle = metadata.getTableHandle(SESSION, new SchemaTableName("db", "orders"));

        ConnectorTableMetadata tableMetadata = metadata.getTableMetadata(SESSION, handle);

        assertThat(tableMetadata.table()).isEqualTo(new SchemaTableName("db", "orders"));
        assertThat(tableMetadata.columns()).containsExactly("id", "name", "amount");
        assertThat(tableMetadata.typedColumns()).hasSize(3);
        assertTypedColumn(tableMetadata.typedColumns().get(0), "id", PangrovaType.BIGINT, 1);
        assertTypedColumn(tableMetadata.typedColumns().get(1), "name", PangrovaType.VARCHAR, 2);
        assertTypedColumn(tableMetadata.typedColumns().get(2), "amount", PangrovaType.DOUBLE, 3);
    }

    @Test
    void getColumnHandles_returnsHandlesWithFieldIds() {
        createNamespace("db");
        catalog.createTable(TableIdentifier.of("db", "orders"), TEST_SCHEMA);
        ConnectorTableHandle handle = metadata.getTableHandle(SESSION, new SchemaTableName("db", "orders"));

        Map<String, ColumnHandle> columns = metadata.getColumnHandles(SESSION, handle);

        assertThat(columns).hasSize(3).containsKeys("id", "name", "amount");
        assertColumnHandle(columns.get("id"), 1, "id", PangrovaType.BIGINT);
        assertColumnHandle(columns.get("name"), 2, "name", PangrovaType.VARCHAR);
        assertColumnHandle(columns.get("amount"), 3, "amount", PangrovaType.DOUBLE);
    }

    @Test
    void listSnapshotReferences_readsCatalogMetadataWithoutSystemTableRows() {
        createNamespace("db");
        var table = catalog.createTable(TableIdentifier.of("db", "orders"), TEST_SCHEMA);
        var append = table.newFastAppend();
        // A branch can be created only after the first snapshot exists.
        var dataFile = org.apache.iceberg.DataFiles.builder(table.spec())
                .withPath(table.location() + "/data/refs.parquet")
                .withFormat(org.apache.iceberg.FileFormat.PARQUET)
                .withRecordCount(1)
                .withFileSizeInBytes(32)
                .build();
        append.appendFile(dataFile).commit();
        long snapshotId = table.currentSnapshot().snapshotId();
        table.manageSnapshots().createBranch("feature", snapshotId).createTag("release", snapshotId).commit();

        ConnectorTableHandle handle = metadata.getTableHandle(
                SESSION, new SchemaTableName("db", "orders"));
        assertThat(metadata.listSnapshotReferences(SESSION, handle)).containsExactlyInAnyOrder(
                new SnapshotReference("main", SnapshotReference.Type.BRANCH, snapshotId),
                new SnapshotReference("feature", SnapshotReference.Type.BRANCH, snapshotId),
                new SnapshotReference("release", SnapshotReference.Type.TAG, snapshotId));
    }

    @Test
    void listSnapshotReferences_emptyTableReturnsEmptyCatalogList() {
        createNamespace("db");
        catalog.createTable(TableIdentifier.of("db", "empty_orders"), TEST_SCHEMA);
        ConnectorTableHandle handle = metadata.getTableHandle(
                SESSION, new SchemaTableName("db", "empty_orders"));

        assertThat(metadata.listSnapshotReferences(SESSION, handle)).isEmpty();
    }

    @Test
    void conditionalBranchDropDoesNotDeleteBranchAdvancedSinceListing() {
        createNamespace("db");
        var identifier = TableIdentifier.of("db", "orders");
        var table = catalog.createTable(identifier, TEST_SCHEMA);
        appendSyntheticFile(table, "first.parquet");
        long observedSnapshotId = table.currentSnapshot().snapshotId();
        table.manageSnapshots().createBranch("feature", observedSnapshotId)
                .createTag("release", observedSnapshotId).commit();
        ConnectorTableHandle handle = metadata.getTableHandle(
                SESSION, new SchemaTableName("db", "orders"));

        appendSyntheticFile(table, "second.parquet");
        long advancedSnapshotId = table.currentSnapshot().snapshotId();
        table.manageSnapshots().replaceBranch("feature", advancedSnapshotId).commit();

        assertThat(metadata.dropBranchIfSnapshotMatches(
                SESSION, handle, "feature", observedSnapshotId))
                .isEqualTo(ConditionalBranchDropResult.REF_CHANGED);
        assertThat(catalog.loadTable(identifier).refs().get("feature").snapshotId())
                .isEqualTo(advancedSnapshotId);
        assertThat(metadata.dropBranchIfSnapshotMatches(
                SESSION, handle, "release", observedSnapshotId))
                .isEqualTo(ConditionalBranchDropResult.REF_CHANGED);
        assertThat(metadata.dropBranchIfSnapshotMatches(
                SESSION, handle, "main", advancedSnapshotId))
                .isEqualTo(ConditionalBranchDropResult.REF_CHANGED);
        assertThat(metadata.dropBranchIfSnapshotMatches(
                SESSION, handle, "feature", advancedSnapshotId))
                .isEqualTo(ConditionalBranchDropResult.DROPPED);
        assertThat(catalog.loadTable(identifier).refs()).containsKey("release").doesNotContainKey("feature");
    }

    @Test
    void conditionalBranchDropReturnsConflictAndUnknownWithoutReplayingCommit() {
        createNamespace("db");
        var identifier = TableIdentifier.of("db", "orders");
        var table = catalog.createTable(identifier, TEST_SCHEMA);
        appendSyntheticFile(table, "first.parquet");
        long snapshotId = table.currentSnapshot().snapshotId();
        table.manageSnapshots().createBranch("feature", snapshotId).commit();
        IcebergTableHandle handle = (IcebergTableHandle) metadata.getTableHandle(
                SESSION, new SchemaTableName("db", "orders"));

        assertThat(IcebergRefOperations.dropBranchIfSnapshotMatches(
                operationsWithFailingCommit(table, new CommitFailedException("simulated conflict")),
                handle, "feature", snapshotId))
                .isEqualTo(ConditionalBranchDropResult.RETRYABLE_CONFLICT);
        assertThat(IcebergRefOperations.dropBranchIfSnapshotMatches(
                operationsWithFailingCommit(table,
                        new CommitStateUnknownException(new IllegalStateException("lost acknowledgement"))),
                handle, "feature", snapshotId))
                .isEqualTo(ConditionalBranchDropResult.COMMIT_STATE_UNKNOWN);
        assertThat(catalog.loadTable(identifier).refs()).containsKey("feature");
    }

    @Test
    void conditionalBranchDropFailsClosedForUnverifiedCatalogOperations() {
        createNamespace("db");
        var table = catalog.createTable(TableIdentifier.of("db", "orders"), TEST_SCHEMA);
        appendSyntheticFile(table, "first.parquet");
        long snapshotId = table.currentSnapshot().snapshotId();
        table.manageSnapshots().createBranch("feature", snapshotId).commit();
        IcebergTableHandle handle = (IcebergTableHandle) metadata.getTableHandle(
                SESSION, new SchemaTableName("db", "orders"));

        Table unverifiedTable = tableWithOperations(
                operationsWithFailingCommit(table, new CommitFailedException("must not commit")));
        assertThatThrownBy(() -> IcebergRefOperations.dropBranchIfSnapshotMatches(
                catalog, unverifiedTable, handle, "feature", snapshotId))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessageContaining("not verified");
        assertThat(catalog.loadTable(TableIdentifier.of("db", "orders")).refs())
                .containsKey("feature");
    }

    @Test
    void conditionalBranchDropFailsClosedForUnverifiedStorage() {
        createNamespace("db");
        var identifier = TableIdentifier.of("db", "orders");
        var table = catalog.createTable(identifier, TEST_SCHEMA);
        appendSyntheticFile(table, "first.parquet");
        long snapshotId = table.currentSnapshot().snapshotId();
        table.manageSnapshots().createBranch("feature", snapshotId).commit();
        IcebergTableHandle handle = (IcebergTableHandle) metadata.getTableHandle(
                SESSION, new SchemaTableName("db", "orders"));

        for (String location : List.of("s3://bucket/table", "s3a://bucket/table",
                "gs://bucket/table", "abfs://container/table", "hdfs://namenode/table")) {
            assertThatThrownBy(() -> IcebergRefOperations.dropBranchIfSnapshotMatches(
                    catalog, tableWithOperations(((HasTableOperations) table).operations(), location),
                    handle, "feature", snapshotId))
                    .isInstanceOf(UnsupportedOperationException.class)
                    .hasMessageContaining("atomic metadata rename");
        }
        assertThat(catalog.loadTable(identifier).refs()).containsKey("feature");
    }

    @Test
    void conditionalBranchDropRejectsAdvanceBetweenRefreshAndCommit() {
        createNamespace("db");
        var identifier = TableIdentifier.of("db", "orders");
        var table = catalog.createTable(identifier, TEST_SCHEMA);
        appendSyntheticFile(table, "first.parquet");
        long observedSnapshotId = table.currentSnapshot().snapshotId();
        table.manageSnapshots().createBranch("feature", observedSnapshotId).commit();
        IcebergTableHandle handle = (IcebergTableHandle) metadata.getTableHandle(
                SESSION, new SchemaTableName("db", "orders"));
        TableOperations delegate = ((HasTableOperations) table).operations();
        AtomicLong advancedSnapshotId = new AtomicLong();
        TableOperations racingOperations = (TableOperations) Proxy.newProxyInstance(
                TableOperations.class.getClassLoader(), new Class<?>[]{TableOperations.class},
                (proxy, method, args) -> switch (method.getName()) {
                    case "refresh" -> delegate.refresh();
                    case "commit" -> {
                        appendSyntheticFile(table, "advanced.parquet");
                        long newSnapshotId = table.currentSnapshot().snapshotId();
                        table.manageSnapshots().replaceBranch("feature", newSnapshotId).commit();
                        advancedSnapshotId.set(newSnapshotId);
                        delegate.commit((TableMetadata) args[0], (TableMetadata) args[1]);
                        yield null;
                    }
                    default -> throw new AssertionError("Unexpected TableOperations call: " + method.getName());
                });

        assertThat(IcebergRefOperations.dropBranchIfSnapshotMatches(
                racingOperations, handle, "feature", observedSnapshotId))
                .isEqualTo(ConditionalBranchDropResult.RETRYABLE_CONFLICT);
        assertThat(catalog.loadTable(identifier).refs().get("feature").snapshotId())
                .isEqualTo(advancedSnapshotId.get());
        assertThat(advancedSnapshotId.get()).isNotEqualTo(observedSnapshotId);
    }

    private static TableOperations operationsWithFailingCommit(Table table, RuntimeException failure) {
        TableOperations delegate = ((HasTableOperations) table).operations();
        TableOperations failingOperations = (TableOperations) Proxy.newProxyInstance(
                TableOperations.class.getClassLoader(), new Class<?>[]{TableOperations.class},
                (proxy, method, args) -> switch (method.getName()) {
                    case "refresh" -> delegate.refresh();
                    case "commit" -> throw failure;
                    default -> throw new AssertionError("Unexpected TableOperations call: " + method.getName());
                });
        return failingOperations;
    }

    private static Table tableWithOperations(TableOperations operations) {
        return tableWithOperations(operations, "file:/verified-local-table");
    }

    private static Table tableWithOperations(TableOperations operations, String location) {
        return (Table) Proxy.newProxyInstance(Table.class.getClassLoader(),
                new Class<?>[]{Table.class, HasTableOperations.class},
                (proxy, method, args) -> {
                    if ("operations".equals(method.getName())) {
                        return operations;
                    }
                    if ("location".equals(method.getName())) {
                        return location;
                    }
                    throw new AssertionError("Unexpected Table call: " + method.getName());
                });
    }

    private static void appendSyntheticFile(org.apache.iceberg.Table table, String fileName) {
        var dataFile = org.apache.iceberg.DataFiles.builder(table.spec())
                .withPath(table.location() + "/data/" + fileName)
                .withFormat(org.apache.iceberg.FileFormat.PARQUET)
                .withRecordCount(1)
                .withFileSizeInBytes(32)
                .build();
        table.newFastAppend().appendFile(dataFile).commit();
    }

    @Test
    void systemTablesProducePinnedTypedV2MetadataSplits() throws Exception {
        createNamespace("db");
        catalog.createTable(TableIdentifier.of("db", "orders"), TEST_SCHEMA);
        IcebergSplitManager splitManager = new IcebergSplitManager(
                "test_catalog", catalog, new IcebergExpressionToSubstrait());

        for (String suffix : List.of("snapshots", "history")) {
            ConnectorTableHandle handle = metadata.getTableHandle(
                    SESSION, new SchemaTableName("db", "orders$" + suffix));
            assertThat(handle).isInstanceOf(IcebergSystemTableHandle.class);
            IcebergSystemTableHandle system = (IcebergSystemTableHandle) handle;
            assertThat(system.metadataLocation()).isNotBlank();
            assertThat(system.metadataLength()).isPositive();
            assertThat(metadata.getTableMetadata(SESSION, system).typedColumns())
                    .containsExactlyElementsOf(system.typedColumns());

            var source = splitManager.getSplits(ConnectorTransactionHandle.EMPTY,
                    SESSION, system, DynamicFilter.ALL, Constraint.EMPTY);
            try {
                var splits = source.getNextBatch(10);
                assertThat(splits).singleElement().isInstanceOf(IcebergSystemTableSplit.class);
                DataSplit proto = DataSplit.parseFrom(splits.get(0).toSplitInfoProto());
                assertThat(proto.getConnectorId()).isEqualTo("iceberg");
                assertThat(proto.getOpaqueLocation()).isEqualTo(system.metadataLocation());
                assertThat(proto.getSourceSnapshot()).isEqualTo(system.metadataLocation());
                assertThat(proto.getLength()).isEqualTo(system.metadataLength());
                var detail = DataSplitProtoCodec.fileDetail(proto);
                assertThat(detail.getFileFormat()).isEqualTo(FileFormat.FILE_FORMAT_METADATA_JSON);
                assertThat(detail.getSystemTable().getKind()).isEqualTo(
                        suffix.equals("snapshots")
                                ? SystemTableSplitDetail.Kind.ICEBERG_SNAPSHOTS
                                : SystemTableSplitDetail.Kind.ICEBERG_HISTORY);
            } finally {
                source.close();
            }
        }
    }

    private Catalog createHadoopCatalog() {
        Map<String, String> props = Map.of(
                "type", "hadoop",
                "warehouse", warehouse.toString());
        org.apache.hadoop.conf.Configuration hadoopConf = new org.apache.hadoop.conf.Configuration();
        return org.apache.iceberg.CatalogUtil.buildIcebergCatalog("test_catalog", props, hadoopConf);
    }

    private void createNamespace(String name) {
        if (catalog instanceof org.apache.iceberg.catalog.SupportsNamespaces nsCatalog) {
            nsCatalog.createNamespace(Namespace.of(name));
        }
    }

    private void assertTypedColumn(ConnectorTableMetadata.Column col, String name, PangrovaType type, int fieldId) {
        assertThat(col.name()).isEqualTo(name);
        assertThat(col.type()).isEqualTo(type);
        assertThat(col.fieldId()).hasValue(fieldId);
    }

    private void assertColumnHandle(ColumnHandle handle, int fieldId, String name, PangrovaType type) {
        assertThat(handle).isInstanceOf(IcebergColumnHandle.class);
        IcebergColumnHandle col = (IcebergColumnHandle) handle;
        assertThat(col.fieldId()).isEqualTo(fieldId);
        assertThat(col.name()).isEqualTo(name);
        assertThat(col.type()).isEqualTo(type);
    }
}
