package org.pangrova.testing.e2e.connector;

import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.parquet.GenericParquetWriter;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.io.DataWriter;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.pangrova.metadata.control.JdbcTableControlStore;
import org.pangrova.metadata.control.OwnershipMode;
import org.pangrova.metadata.control.TableControlService;
import org.pangrova.metadata.control.TableControlSettings;
import org.pangrova.metadata.control.TableControlState;
import org.pangrova.testing.e2e.SqlE2ETestBase;
import org.pangrova.testing.e2e.config.ConnectorTestConfig;

import java.util.List;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/** HTTP SQL, native reads/writes, SDK external writer and durable ownership fences. */
class TableControlLifecycleE2ETest extends SqlE2ETestBase {
    private static final String CATALOG = "control_catalog";
    private static final String SCHEMA = "default";
    private static final String PAIMON = "control_paimon";
    private static final Schema TABLE_SCHEMA = new Schema(
            Types.NestedField.optional(1, "id", Types.LongType.get()));

    private TableControlService previousControl;
    private JdbcTableControlStore store;
    private HadoopCatalog externalCatalog;

    @Override
    protected List<ConnectorTestConfig> connectorConfigs() {
        return List.of(ConnectorTestConfig.icebergLocal(
                CATALOG, SCHEMA, icebergWarehouse().toString()),
                ConnectorTestConfig.paimon(PAIMON, SCHEMA, paimonWarehouse().toString()));
    }

    @BeforeEach
    void enableDurableEnforcement() {
        var registry = metadataBootstrap.catalogRegistry();
        previousControl = registry.tableControl();
        store = new JdbcTableControlStore(stateStoreDataSource());
        registry.setTableControlService(new TableControlService(store,
                new TableControlSettings(true, true, Set.of())));
        externalCatalog = new HadoopCatalog(new Configuration(), icebergWarehouse().toString());
    }

    @AfterEach
    void restoreEnforcement() throws Exception {
        metadataBootstrap.catalogRegistry().setTableControlService(previousControl);
        externalCatalog.close();
    }

    @Test
    void externalDataRemainsReadableAcrossAdoptAndUnlinkButNeverWritable() throws Exception {
        String table = "control_external_lifecycle";
        Table external = externalCatalog.createTable(identifier(table), TABLE_SCHEMA);
        append(external, 41L);

        assertThat(sqlRunner.execute("SELECT id FROM " + fqn(table)).get(0).getLong("id"))
                .isEqualTo(41L);
        assertThat(store.find(CATALOG, SCHEMA, table)).isEmpty();
        assertThat(sqlRunner.executeExpectingError("INSERT INTO " + fqn(table) + " VALUES (42)").message())
                .contains("OBSERVED");
        assertThat(sqlRunner.executeExpectingError("DROP TABLE " + fqn(table)).message())
                .contains("OBSERVED");
        assertThat(sqlRunner.executeExpectingError(
                "CALL " + CATALOG + ".compact('" + SCHEMA + "." + table + "')").message())
                .contains("OBSERVED");

        sqlRunner.executeDdl("ALTER TABLE " + fqn(table) + " ADOPT");
        var linked = store.find(CATALOG, SCHEMA, table).orElseThrow();
        assertThat(linked.ownershipMode()).isEqualTo(OwnershipMode.LINKED);
        assertThat(new JdbcTableControlStore(stateStoreDataSource()).findById(linked.tableId()))
                .contains(linked);
        assertThat(sqlRunner.executeExpectingError("INSERT INTO " + fqn(table) + " VALUES (42)").message())
                .contains("LINKED");
        assertThat(sqlRunner.executeExpectingError("DELETE FROM " + fqn(table)).message())
                .contains("LINKED");
        assertThat(sqlRunner.executeExpectingError("DROP TABLE " + fqn(table)).message())
                .contains("LINKED");
        assertThat(sqlRunner.executeExpectingError(
                "CALL " + CATALOG + ".compact('" + SCHEMA + "." + table + "')").message())
                .contains("not enabled");

        sqlRunner.executeDdl("ALTER TABLE " + fqn(table) + " UNLINK");
        assertThat(store.find(CATALOG, SCHEMA, table)).isEmpty();
        assertThat(externalCatalog.tableExists(identifier(table))).isTrue();
        assertThat(sqlRunner.execute("SELECT id FROM " + fqn(table)))
                .singleElement().satisfies(row -> assertThat(row.getLong("id")).isEqualTo(41L));
    }

    @Test
    void managedCallAndAnalyzeExecuteThroughTheControlPlane() throws Exception {
        String table = "control_managed_maintenance";
        sqlRunner.executeDdl("CREATE TABLE " + fqn(table) + " (id BIGINT)");
        sqlRunner.execute("INSERT INTO " + fqn(table) + " VALUES (7)");
        sqlRunner.execute("INSERT INTO " + fqn(table) + " VALUES (8)");

        assertThat(sqlRunner.execute("CALL " + CATALOG + ".compact('" + SCHEMA + "." + table + "')"))
                .hasSize(1);
        assertThat(sqlRunner.execute("ANALYZE TABLE " + fqn(table)))
                .singleElement().satisfies(row -> assertThat(row.getLong("row_count")).isEqualTo(2L));
        var record = store.find(CATALOG, SCHEMA, table).orElseThrow();
        assertThat(metadataBootstrap.catalogRegistry().tableStatistics().fresh(record))
                .hasValueSatisfying(stats -> assertThat(stats.statistics().rowCount()).isEqualTo(2L));
        assertThat(sqlRunner.executeExpectingError("CALL " + CATALOG
                + ".expire_snapshots('" + SCHEMA + "." + table + "', 1)").message())
                .contains("retention policy");
        assertThat(sqlRunner.execute("SELECT id FROM " + fqn(table) + " ORDER BY id"))
                .extracting(row -> row.getLong("id")).containsExactly(7L, 8L);
    }

    @Test
    void managedWritesSucceedButDropOfAnExternalReplacementIsRejected() throws Exception {
        String table = "control_managed_replacement";
        sqlRunner.executeDdl("CREATE TABLE " + fqn(table) + " (id BIGINT)");
        sqlRunner.execute("INSERT INTO " + fqn(table) + " VALUES (1)");
        assertThat(sqlRunner.execute("SELECT id FROM " + fqn(table)).get(0).getLong("id"))
                .isEqualTo(1L);
        assertThat(store.find(CATALOG, SCHEMA, table).orElseThrow().ownershipMode())
                .isEqualTo(OwnershipMode.MANAGED);

        externalCatalog.dropTable(identifier(table), true);
        Table replacement = externalCatalog.createTable(identifier(table), TABLE_SCHEMA);
        append(replacement, 99L);
        assertThat(sqlRunner.executeExpectingError("DROP TABLE " + fqn(table)).message())
                .contains("identity");

        assertThat(externalCatalog.tableExists(identifier(table))).isTrue();
        assertThat(store.find(CATALOG, SCHEMA, table).orElseThrow().state())
                .isEqualTo(TableControlState.QUARANTINED);
        assertThat(externalCatalog.loadTable(identifier(table)).currentSnapshot().summary())
                .containsEntry("total-records", "1");
    }

    @Test
    void managedPaimonRenamePreservesOwnershipAndRefreshesPhysicalIdentity() {
        String before = PAIMON + "." + SCHEMA + ".control_before_rename";
        String after = PAIMON + "." + SCHEMA + ".control_after_rename";
        sqlRunner.executeDdl("CREATE TABLE " + before + " (id BIGINT)");
        sqlRunner.execute("INSERT INTO " + before + " VALUES (7)");
        sqlRunner.execute("SELECT id FROM " + before);
        var original = store.find(PAIMON, SCHEMA, "control_before_rename").orElseThrow();

        sqlRunner.executeDdl("ALTER TABLE " + before + " RENAME TO " + after);
        var renamed = store.find(PAIMON, SCHEMA, "control_after_rename").orElseThrow();
        assertThat(store.find(PAIMON, SCHEMA, "control_before_rename")).isEmpty();
        assertThat(renamed.tableId()).isEqualTo(original.tableId());
        assertThat(renamed.state()).isEqualTo(TableControlState.ACTIVE);
        assertThat(renamed.formatLocation()).isNotEqualTo(original.formatLocation());
        sqlRunner.execute("INSERT INTO " + after + " VALUES (8)");
        assertThat(sqlRunner.execute("SELECT id FROM " + after + " ORDER BY id"))
                .extracting(row -> row.getLong("id")).containsExactly(7L, 8L);
        sqlRunner.executeDdl("DROP TABLE " + after);
        assertThat(store.findById(original.tableId()).orElseThrow().state())
                .isEqualTo(TableControlState.DROPPED);
    }

    @Test
    void unqualifiedEqualityDeleteReadsFailClosedBeforeNativeExecution() throws Exception {
        String table = "control_external_equality_delete";
        Table external = externalCatalog.createTable(identifier(table), TABLE_SCHEMA);
        append(external, 41L);
        var deleted = GenericRecord.create(TABLE_SCHEMA);
        deleted.setField("id", 41L);
        var deleteWriter = Parquet.writeDeletes(external.io().newOutputFile(
                        external.location() + "/data/external-equality-delete.parquet"))
                .forTable(external).withSpec(external.spec())
                .createWriterFunc(GenericParquetWriter::create)
                .equalityFieldIds(new int[] {1}).rowSchema(TABLE_SCHEMA)
                .buildEqualityWriter();
        try (deleteWriter) { deleteWriter.write(deleted); }
        external.newRowDelta().addDeletes(deleteWriter.toDeleteFile()).commit();

        assertThat(sqlRunner.executeExpectingError("SELECT id FROM " + fqn(table)).message())
                .contains("equality deletes").contains(table);
        assertThat(sqlRunner.executeExpectingError("SELECT COUNT(*) FROM " + fqn(table)).message())
                .contains("equality deletes").contains(table);
    }

    @Test
    void externalSdkDeletionVectorsSelectTheBlobForEachDataFile() throws Exception {
        String name = "control_external_deletion_vectors";
        Table table = externalCatalog.buildTable(identifier(name), TABLE_SCHEMA)
                .withProperty("format-version", "3").create();
        var first = append(table, 11L, 12L);
        var second = append(table, 21L, 22L);
        var writer = new org.apache.iceberg.deletes.BaseDVFileWriter(
                () -> table.io().newOutputFile(table.location() + "/data/external-dv.puffin"),
                ignored -> null);
        try (writer) {
            writer.delete(first.path().toString(), 0, table.spec(), first.partition());
            writer.delete(second.path().toString(), 1, table.spec(), second.partition());
        }
        var deletes = writer.result().deleteFiles();
        assertThat(deletes).hasSize(2);
        assertThat(deletes.get(0).path()).isEqualTo(deletes.get(1).path());
        var commit = table.newRowDelta();
        deletes.forEach(commit::addDeletes);
        commit.commit();
        assertThat(sqlRunner.execute("SELECT id FROM " + fqn(name) + " ORDER BY id"))
                .extracting(row -> row.getLong("id")).containsExactly(12L, 21L);
        assertThat(sqlRunner.execute("SELECT COUNT(*) AS n FROM " + fqn(name)))
                .singleElement().satisfies(row -> assertThat(row.getLong("n")).isEqualTo(2L));
    }

    private static org.apache.iceberg.DataFile append(Table table, long... ids) throws Exception {
        DataWriter<Record> writer = Parquet.writeData(
                        table.io().newOutputFile(table.location() + "/data/external-"
                                + java.util.UUID.randomUUID() + ".parquet"))
                .schema(table.schema()).withSpec(table.spec())
                .createWriterFunc(GenericParquetWriter::create).build();
        try (writer) {
            for (long id : ids) {
                GenericRecord row = GenericRecord.create(table.schema());
                row.setField("id", id);
                writer.write(row);
            }
        }
        table.newAppend().appendFile(writer.toDataFile()).commit();
        return writer.toDataFile();
    }

    private static TableIdentifier identifier(String table) {
        return TableIdentifier.of(SCHEMA, table);
    }

    private static String fqn(String table) {
        return CATALOG + "." + SCHEMA + "." + table;
    }
}
