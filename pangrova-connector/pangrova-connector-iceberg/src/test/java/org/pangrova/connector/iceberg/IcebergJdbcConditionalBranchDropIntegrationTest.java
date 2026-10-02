package org.pangrova.connector.iceberg;

import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.HasTableOperations;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.parquet.GenericParquetWriter;
import org.apache.iceberg.io.DataWriter;
import org.apache.iceberg.jdbc.JdbcCatalog;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.pangrova.common.concurrent.ManagedExecutorFactory;
import org.pangrova.connector.iceberg.handle.IcebergTableHandle;
import org.pangrova.spi.connector.ConnectorSession;
import org.pangrova.spi.connector.metadata.ConditionalBranchDropResult;
import org.pangrova.spi.connector.metadata.table.SchemaTableName;
import org.pangrova.testing.ContainerTestBase;

import java.io.IOException;
import java.nio.file.Path;
import java.sql.DriverManager;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/** Actual PostgreSQL metadata-pointer CAS through two independent SDK catalog instances. */
class IcebergJdbcConditionalBranchDropIntegrationTest extends ContainerTestBase {

    private static final Schema SCHEMA = new Schema(Types.NestedField.required(1, "id", Types.LongType.get()));
    private static final TableIdentifier IDENTIFIER = TableIdentifier.of("db", "events");
    private static final ConnectorSession SESSION = new ConnectorSession() {
        @Override public String getUser() { return "test"; }
        @Override public Optional<String> getCatalog() { return Optional.of("jdbc_catalog"); }
        @Override public Optional<String> getSchema() { return Optional.of("db"); }
        @Override public Map<String, String> getProperties() { return Map.of(); }
    };

    @TempDir
    Path warehouse;

    private JdbcCatalog firstCatalog;
    private JdbcCatalog secondCatalog;
    private IcebergMetadata metadata;
    private IcebergTableHandle handle;
    private Table firstTable;
    private long observedSnapshotId;

    @BeforeEach
    void createRealCatalogsAndBranch() throws Exception {
        String catalogName = "jdbc_cas_" + UUID.randomUUID().toString().replace("-", "");
        firstCatalog = createCatalog(catalogName);
        secondCatalog = createCatalog(catalogName);
        firstCatalog.createNamespace(Namespace.of("db"));
        firstTable = firstCatalog.createTable(IDENTIFIER, SCHEMA);
        appendRow(firstTable, 1);
        observedSnapshotId = firstTable.currentSnapshot().snapshotId();
        firstTable.manageSnapshots().createBranch("feature", observedSnapshotId)
                .createBranch("other", observedSnapshotId).createTag("release", observedSnapshotId).commit();
        metadata = new IcebergMetadata(firstCatalog, catalogName);
        handle = (IcebergTableHandle) metadata.getTableHandle(SESSION, new SchemaTableName("db", "events"));
    }

    @AfterEach
    void closeCatalogResources() throws IOException {
        try {
            if (secondCatalog != null) {
                secondCatalog.close();
            }
        } finally {
            if (firstCatalog != null) {
                firstCatalog.close();
            }
        }
    }

    @Test
    void staleBranchCannotBeDroppedAndCurrentBranchCanBeDropped() throws Exception {
        Table advanced = secondCatalog.loadTable(IDENTIFIER);
        appendRow(advanced, 2);
        long newSnapshotId = advanced.currentSnapshot().snapshotId();
        advanced.manageSnapshots().replaceBranch("feature", newSnapshotId).commit();

        assertThat(metadata.dropBranchIfSnapshotMatches(SESSION, handle, "feature", observedSnapshotId))
                .isEqualTo(ConditionalBranchDropResult.REF_CHANGED);
        assertThat(secondCatalog.loadTable(IDENTIFIER).refs().get("feature").snapshotId()).isEqualTo(newSnapshotId);
        assertThat(metadata.dropBranchIfSnapshotMatches(SESSION, handle, "feature", newSnapshotId))
                .isEqualTo(ConditionalBranchDropResult.DROPPED);
        assertThat(secondCatalog.loadTable(IDENTIFIER).refs()).doesNotContainKey("feature").containsKey("release");
    }

    @Test
    void sameSnapshotTagReplacementCannotBeDeletedFromCheckedBranchBase() {
        TableOperations checkedOperations = operations(firstTable);
        var checkedBase = checkedOperations.refresh();
        Table replacement = secondCatalog.loadTable(IDENTIFIER);
        replacement.manageSnapshots().removeBranch("feature").commit();
        replacement.manageSnapshots().createTag("feature", observedSnapshotId).commit();

        assertThat(IcebergRefOperations.commitBranchRemoval(
                checkedOperations, checkedBase, handle, "feature", observedSnapshotId))
                .isEqualTo(ConditionalBranchDropResult.RETRYABLE_CONFLICT);
        var ref = secondCatalog.loadTable(IDENTIFIER).refs().get("feature");
        assertThat(ref.isTag()).isTrue();
        assertThat(ref.snapshotId()).isEqualTo(observedSnapshotId);
        assertThat(metadata.dropBranchIfSnapshotMatches(SESSION, handle, "feature", observedSnapshotId))
                .isEqualTo(ConditionalBranchDropResult.REF_CHANGED);
    }

    @Test
    void concurrentCommitsAgainstOneExactBaseHaveOnlyOneWinner() throws Exception {
        TableOperations firstOperations = operations(firstTable);
        TableOperations secondOperations = operations(secondCatalog.loadTable(IDENTIFIER));
        var firstBase = firstOperations.refresh();
        var secondBase = secondOperations.refresh();
        assertThat(firstBase.metadataFileLocation()).isEqualTo(secondBase.metadataFileLocation());
        CountDownLatch ready = new CountDownLatch(2);
        CountDownLatch start = new CountDownLatch(1);
        var executor = ManagedExecutorFactory.newFixedThreadPool("iceberg-jdbc-ref-cas-test", 2);
        try {
            var first = executor.submit(() -> {
                ready.countDown();
                assertThat(start.await(30, TimeUnit.SECONDS)).isTrue();
                return IcebergRefOperations.commitBranchRemoval(
                        firstOperations, firstBase, handle, "feature", observedSnapshotId);
            });
            var second = executor.submit(() -> {
                ready.countDown();
                assertThat(start.await(30, TimeUnit.SECONDS)).isTrue();
                return IcebergRefOperations.commitBranchRemoval(
                        secondOperations, secondBase, handle, "other", observedSnapshotId);
            });
            assertThat(ready.await(30, TimeUnit.SECONDS)).isTrue();
            start.countDown();
            assertThat(List.of(first.get(30, TimeUnit.SECONDS), second.get(30, TimeUnit.SECONDS)))
                    .containsExactlyInAnyOrder(ConditionalBranchDropResult.DROPPED,
                            ConditionalBranchDropResult.RETRYABLE_CONFLICT);
            var refs = firstCatalog.loadTable(IDENTIFIER).refs();
            assertThat(refs.containsKey("feature") ^ refs.containsKey("other")).isTrue();
            assertThat(refs).containsKeys("main", "release");
        } finally {
            start.countDown();
            executor.shutdownNow();
        }
    }

    @Test
    void protectedAndInvalidReferencesAreNotCommitted() {
        for (String branch : new String[] {"main", "release", "missing", "", " ", null}) {
            assertThat(metadata.dropBranchIfSnapshotMatches(SESSION, handle, branch, observedSnapshotId))
                    .isEqualTo(ConditionalBranchDropResult.REF_CHANGED);
        }
        assertThat(metadata.dropBranchIfSnapshotMatches(SESSION, handle, "feature", -1))
                .isEqualTo(ConditionalBranchDropResult.REF_CHANGED);
        assertThat(firstCatalog.loadTable(IDENTIFIER).refs()).containsKeys("main", "feature", "release");
    }

    @Test
    void jdbcCasUsesMetadataPointerInsteadOfTableLocationScheme() {
        // Only metadata is accessed by TTL: keep its real files local while
        // relocating the table root through the SDK, without an S3 fake/FileIO.
        firstTable.updateProperties().set(TableProperties.WRITE_METADATA_LOCATION,
                warehouse.resolve("metadata").toString()).commit();
        firstTable.updateLocation().setLocation("s3://iceberg-test-bucket/events").commit();
        assertThat(firstCatalog.loadTable(IDENTIFIER).location()).startsWith("s3://");
        assertThat(metadata.dropBranchIfSnapshotMatches(SESSION, handle, "feature", observedSnapshotId))
                .isEqualTo(ConditionalBranchDropResult.DROPPED);
        assertThat(secondCatalog.loadTable(IDENTIFIER).refs()).doesNotContainKey("feature");
    }

    @Test
    void productionCatalogUtilDiscoversRuntimePostgresDriver() throws Exception {
        assertThat(firstCatalog.getClass()).isEqualTo(JdbcCatalog.class);
        var postgres = IcebergJdbcTestDatabase.instance();
        assertThat(DriverManager.getDriver(postgres.getJdbcUrl()).acceptsURL(postgres.getJdbcUrl())).isTrue();
        try (var connection = DriverManager.getConnection(
                postgres.getJdbcUrl(), postgres.getUsername(), postgres.getPassword());
                var statement = connection.prepareStatement(
                        "SELECT metadata_location FROM iceberg_tables WHERE catalog_name = ? AND table_name = ?")) {
            statement.setString(1, firstCatalog.name());
            statement.setString(2, IDENTIFIER.name());
            try (var result = statement.executeQuery()) {
                assertThat(result.next()).isTrue();
                assertThat(result.getString(1)).isEqualTo(operations(firstTable).refresh().metadataFileLocation());
                assertThat(result.next()).isFalse();
            }
        }
    }

    private JdbcCatalog createCatalog(String catalogName) {
        var postgres = IcebergJdbcTestDatabase.instance();
        Map<String, String> properties = Map.of("type", "jdbc", "uri", postgres.getJdbcUrl(),
                "warehouse", warehouse.toString(), "jdbc.user", postgres.getUsername(),
                "jdbc.password", postgres.getPassword(), "jdbc.schema-version", "V1");
        Configuration configuration = new Configuration();
        configuration.set("fs.file.impl", org.apache.hadoop.fs.RawLocalFileSystem.class.getName());
        configuration.setBoolean("fs.file.impl.disable.cache", true);
        return (JdbcCatalog) CatalogUtil.buildIcebergCatalog(catalogName, properties, configuration);
    }

    private static TableOperations operations(Table table) {
        return ((HasTableOperations) table).operations();
    }

    private static void appendRow(Table table, long id) throws IOException {
        var output = table.io().newOutputFile(table.location() + "/data/row-" + id + ".parquet");
        DataWriter<Record> writer = Parquet.writeData(output).schema(SCHEMA)
                .createWriterFunc(GenericParquetWriter::create).withSpec(table.spec()).build();
        try (writer) {
            var record = GenericRecord.create(SCHEMA);
            record.setField("id", id);
            writer.write(record);
        }
        table.newAppend().appendFile(writer.toDataFile()).commit();
    }
}
