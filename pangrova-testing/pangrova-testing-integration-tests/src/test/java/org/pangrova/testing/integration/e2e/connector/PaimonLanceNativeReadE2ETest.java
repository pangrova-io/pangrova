package org.pangrova.testing.integration.e2e.connector;

import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.CatalogFactory;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.BinaryVector;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.disk.IOManager;
import org.apache.paimon.options.Options;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.types.DataTypes;
import org.junit.jupiter.api.Test;
import org.pangrova.testing.testkit.E2ETest;
import org.pangrova.testing.integration.e2e.PangrovaE2ETestBase;
import org.pangrova.testing.integration.e2e.config.ConnectorTestConfig;

import java.util.List;
import java.util.UUID;
import java.nio.file.Files;

import static org.assertj.core.api.Assertions.assertThat;

/** SDK 0.39 writes physical files; public SQL reads through the sole current Pangrova Native ABI. */
@E2ETest
class PaimonLanceNativeReadE2ETest extends PangrovaE2ETestBase {
    private static final String CATALOG = "paimon_lance_native";
    private static final String DATABASE = "default";

    @Override
    protected List<ConnectorTestConfig> connectorConfigs() {
        return List.of(ConnectorTestConfig.paimon(CATALOG, DATABASE, warehouse()));
    }

    private static String warehouse() {
        return tempDir.resolve("paimon-lance-native-warehouse").toString();
    }

    @Test
    void sdkPhysicalFilesReadThroughPublicSqlWithProjectionNullsAndVectors() throws Exception {
        String table = "sdk_" + UUID.randomUUID().toString().replace("-", "");
        writeSdkFile(table);
        String name = CATALOG + "." + DATABASE + "." + table;
        assertThat(executeQuery("SELECT id FROM " + name + " WHERE id >= 2 ORDER BY id"))
                .extracting(row -> row.getLong("id")).containsExactly(2L, 3L);
        var rows = executeQuery("SELECT id, label, embedding FROM " + name + " ORDER BY id");
        assertThat(rows).hasSize(3);
        assertThat(rows.get(0).getRaw("label")).isEqualTo("café");
        assertThat(rows.get(1).getRaw("label")).isNull();
        assertThat(rows.get(1).getRaw("embedding")).isNull();
        assertThat((float[]) rows.get(0).getRaw("embedding")).containsExactly(1.0f, -2.0f, 3.5f);
        assertThat((float[]) rows.get(2).getRaw("embedding")).containsExactly(0.0f, 4.0f, -5.0f);
    }

    private static void writeSdkFile(String table) throws Exception {
        Options options = new Options();
        options.setString("warehouse", warehouse());
        try (var catalog = CatalogFactory.createCatalog(CatalogContext.create(options))) {
            catalog.createDatabase(DATABASE, true);
            var id = Identifier.create(DATABASE, table);
            catalog.createTable(id, Schema.newBuilder().column("id", DataTypes.BIGINT())
                    .column("label", DataTypes.STRING()).column("embedding", DataTypes.VECTOR(3, DataTypes.FLOAT()))
                    .option("bucket", "1").option("bucket-key", "id").option("file.format", "lance")
                    .build(), false);
            var source = (FileStoreTable) catalog.getTable(id);
            var write = source.newBatchWriteBuilder();
            try (var writer = write.newWrite(); var commit = write.newCommit()) {
                writer.write(GenericRow.of(1L, BinaryString.fromString("café"),
                        BinaryVector.fromPrimitiveArray(new float[] {1, -2, 3.5f})));
                writer.write(GenericRow.of(2L, null, null));
                writer.write(GenericRow.of(3L, BinaryString.fromString("last"),
                        BinaryVector.fromPrimitiveArray(new float[] {0, 4, -5})));
                commit.commit(writer.prepareCommit());
            }
            assertThat(source.snapshotManager().latestSnapshotId()).isEqualTo(1);
        }
    }

    @Test
    void realSdkPrimaryKeyUpdatesAndTombstonesUsePersistedSequence() throws Exception {
        assertPrimaryKeySnapshot(false, false);
    }

    @Test
    void realSdkDeletionVectorsRemainBoundToEachPhysicalFile() throws Exception {
        assertPrimaryKeySnapshot(true, false);
    }

    @Test
    void realSdkBitmap64DeletionVectorsReadThroughPublicSql() throws Exception {
        assertPrimaryKeySnapshot(true, true);
    }

    private void assertPrimaryKeySnapshot(boolean deletionVectors, boolean bitmap64) throws Exception {
        String table = "pk_" + UUID.randomUUID().toString().replace("-", "");
        Options options = new Options();
        options.setString("warehouse", warehouse());
        try (var catalog = CatalogFactory.createCatalog(CatalogContext.create(options))) {
            var id = Identifier.create(DATABASE, table);
            catalog.createTable(id, Schema.newBuilder().column("id", DataTypes.BIGINT())
                    .column("label", DataTypes.STRING()).primaryKey("id").option("bucket", "1")
                    .option("file.format", "lance")
                    .option("deletion-vectors.enabled", Boolean.toString(deletionVectors))
                    .option("deletion-vectors.bitmap64", Boolean.toString(bitmap64)).build(), false);
            if (deletionVectors) {
                writeLargeDvBase((FileStoreTable) catalog.getTable(id));
            } else {
                writeBatch((FileStoreTable) catalog.getTable(id),
                        GenericRow.of(1L, BinaryString.fromString("old")),
                        GenericRow.of(2L, BinaryString.fromString("deleted")));
            }
            writeBatch((FileStoreTable) catalog.getTable(id), GenericRow.of(1L, BinaryString.fromString("new")),
                    GenericRow.of(3L, BinaryString.fromString("last")),
                    GenericRow.ofKind(org.apache.paimon.types.RowKind.DELETE, 2L, null));
            if (deletionVectors) {
                assertRealDeletionVectors((FileStoreTable) catalog.getTable(id), bitmap64);
            }
        }
        var rows = executeQuery("SELECT id, label FROM " + CATALOG + "." + DATABASE + "." + table
                + " WHERE id < 100 ORDER BY id");
        assertThat(rows).extracting(row -> List.of(row.getLong("id"), row.getString("label")))
                .containsExactly(List.of(1L, "new"), List.of(3L, "last"));
        assertThat(executeQuery("SELECT label FROM " + CATALOG + "." + DATABASE + "." + table
                + " WHERE id < 100 ORDER BY label"))
                .extracting(row -> row.getString("label")).containsExactly("last", "new");
    }

    private static void writeLargeDvBase(FileStoreTable table) throws Exception {
        GenericRow[] rows = new GenericRow[10_002];
        rows[0] = GenericRow.of(1L, BinaryString.fromString("old"));
        rows[1] = GenericRow.of(2L, BinaryString.fromString("deleted"));
        for (int index = 2; index < rows.length; index++) {
            long key = index + 98L;
            rows[index] = GenericRow.of(key, BinaryString.fromString(
                    "seed-" + key + "-" + Long.toHexString(key * 7919L)));
        }
        writeBatch(table, rows);
    }

    private static void assertRealDeletionVectors(FileStoreTable table, boolean bitmap64) throws Exception {
        var scan = table.newReadBuilder().newScan().plan();
        assertThat(scan.splits().stream().map(org.apache.paimon.table.source.DataSplit.class::cast)
                .flatMap(split -> split.dataFiles().stream()).count())
                .as("DV fixture must retain multiple physical data files").isGreaterThan(1);
        var vectors = scan.splits().stream().map(org.apache.paimon.table.source.DataSplit.class::cast)
                .flatMap(split -> split.deletionFiles().orElse(List.of()).stream())
                .filter(java.util.Objects::nonNull).filter(vector -> vector.length() > 0).toList();
        assertThat(vectors).as("Real SDK must publish deletion-vector metadata").isNotEmpty();
        for (var vector : vectors) {
            var decoded = org.apache.paimon.deletionvectors.DeletionVector.read(table.fileIO(), vector);
            assertThat(decoded.getCardinality()).isEqualTo(vector.cardinality());
            assertThat(decoded).isInstanceOf(bitmap64 ? org.apache.paimon.deletionvectors.Bitmap64DeletionVector.class
                    : org.apache.paimon.deletionvectors.BitmapDeletionVector.class);
        }
    }

    private static void writeBatch(FileStoreTable source, GenericRow... rows) throws Exception {
        var write = source.newBatchWriteBuilder();
        var lookupDir = Files.createTempDirectory(tempDir, "paimon-lookup-");
        try (var ioManager = IOManager.create(lookupDir.toString());
             var writer = write.newWrite().withIOManager(ioManager);
             var commit = write.newCommit()) {
            for (var row : rows) writer.write(row);
            commit.commit(writer.prepareCommit());
        }
    }

    @Test
    void partialUpdateDeleteResetDoesNotResurrectFieldsFromOlderPhysicalFiles() throws Exception {
        String table = "partial_" + UUID.randomUUID().toString().replace("-", "");
        Options options = new Options();
        options.setString("warehouse", warehouse());
        try (var catalog = CatalogFactory.createCatalog(CatalogContext.create(options))) {
            var id = Identifier.create(DATABASE, table);
            catalog.createTable(id, Schema.newBuilder().column("id", DataTypes.BIGINT())
                    .column("a", DataTypes.BIGINT()).column("b", DataTypes.BIGINT()).primaryKey("id")
                    .option("bucket", "1").option("file.format", "lance").option("merge-engine", "partial-update")
                    .option("partial-update.remove-record-on-delete", "true").build(), false);
            writeBatch((FileStoreTable) catalog.getTable(id), GenericRow.of(1L, 100L, 200L));
            writeBatch((FileStoreTable) catalog.getTable(id),
                    GenericRow.ofKind(org.apache.paimon.types.RowKind.DELETE, 1L, null, null));
            writeBatch((FileStoreTable) catalog.getTable(id), GenericRow.of(1L, null, 300L));
        }
        var rows = executeQuery("SELECT id, a, b FROM " + CATALOG + "." + DATABASE + "." + table);
        assertThat(rows).hasSize(1);
        assertThat(rows.get(0).getLong("id")).isEqualTo(1L);
        assertThat(rows.get(0).getRaw("a")).isNull();
        assertThat(rows.get(0).getLong("b")).isEqualTo(300L);
    }

    @Test
    void aggregateDefaultsAndDeleteResetUseRealSdkChronologicalFiles() throws Exception {
        String table = "aggregate_" + UUID.randomUUID().toString().replace("-", "");
        Options options = new Options();
        options.setString("warehouse", warehouse());
        try (var catalog = CatalogFactory.createCatalog(CatalogContext.create(options))) {
            var id = Identifier.create(DATABASE, table);
            catalog.createTable(id, Schema.newBuilder().column("id", DataTypes.BIGINT())
                    .column("amount", DataTypes.BIGINT()).column("note", DataTypes.STRING()).primaryKey("id")
                    .option("bucket", "1").option("file.format", "lance").option("merge-engine", "aggregation")
                    .option("fields.amount.aggregate-function", "sum")
                    .option("aggregation.remove-record-on-delete", "true").build(), false);
            writeBatch((FileStoreTable) catalog.getTable(id),
                    GenericRow.of(1L, 10L, BinaryString.fromString("retained")), GenericRow.of(2L, 100L, null));
            writeBatch((FileStoreTable) catalog.getTable(id), GenericRow.of(1L, 3L, null),
                    GenericRow.ofKind(org.apache.paimon.types.RowKind.DELETE, 2L, 0L, null));
            writeBatch((FileStoreTable) catalog.getTable(id), GenericRow.of(2L, 7L, null));
        }
        var rows = executeQuery("SELECT id, amount, note FROM " + CATALOG + "." + DATABASE + "." + table
                + " ORDER BY id");
        assertThat(rows).hasSize(2);
        assertThat(rows.get(0).getLong("amount")).isEqualTo(13L);
        assertThat(rows.get(0).getString("note")).isEqualTo("retained");
        assertThat(rows.get(1).getLong("amount")).isEqualTo(7L);
        assertThat(rows.get(1).getRaw("note")).isNull();
    }

    @Test
    void immutableFieldIdsMapOldLanceFilesAfterRenameNameReuseAndNumericWidening() throws Exception {
        String table = "schema_" + UUID.randomUUID().toString().replace("-", "");
        Options options = new Options();
        options.setString("warehouse", warehouse());
        try (var catalog = CatalogFactory.createCatalog(CatalogContext.create(options))) {
            var id = Identifier.create(DATABASE, table);
            catalog.createTable(id, Schema.newBuilder().column("id", DataTypes.INT()).column("label", DataTypes.STRING())
                    .option("bucket", "1").option("bucket-key", "id").option("file.format", "lance").build(), false);
            writeBatch((FileStoreTable) catalog.getTable(id), GenericRow.of(1, BinaryString.fromString("original")));
            catalog.alterTable(id, List.of(org.apache.paimon.schema.SchemaChange.renameColumn("label", "renamed"),
                    org.apache.paimon.schema.SchemaChange.addColumn("label", DataTypes.STRING()),
                    org.apache.paimon.schema.SchemaChange.updateColumnType("id", DataTypes.BIGINT())), false);
            writeBatch((FileStoreTable) catalog.getTable(id),
                    GenericRow.of(2L, BinaryString.fromString("second"), BinaryString.fromString("fresh")));
        }
        var rows = executeQuery("SELECT id, renamed, label FROM " + CATALOG + "." + DATABASE + "." + table
                + " ORDER BY id");
        assertThat(rows).hasSize(2);
        assertThat(rows.get(0).getLong("id")).isEqualTo(1L);
        assertThat(rows.get(0).getString("renamed")).isEqualTo("original");
        assertThat(rows.get(0).getRaw("label")).isNull();
        assertThat(rows.get(1).getLong("id")).isEqualTo(2L);
        assertThat(rows.get(1).getString("label")).isEqualTo("fresh");
    }
}
