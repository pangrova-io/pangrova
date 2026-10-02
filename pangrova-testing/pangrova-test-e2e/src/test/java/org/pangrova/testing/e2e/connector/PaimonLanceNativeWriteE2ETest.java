package org.pangrova.testing.e2e.connector;

import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.CatalogFactory;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.BinaryVector;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.options.Options;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.types.DataTypes;
import org.junit.jupiter.api.Test;
import org.pangrova.testing.E2ETest;
import org.pangrova.testing.e2e.PangrovaE2ETestBase;
import org.pangrova.testing.e2e.config.ConnectorTestConfig;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/** Actual SDK source -> authenticated public SQL Native write -> SDK/public SQL read.
 * The inherited cluster is single-JVM real HTTP/JNI, not multi-process deployment evidence. */
@E2ETest
class PaimonLanceNativeWriteE2ETest extends PangrovaE2ETestBase {
    private static final String CATALOG = "paimon_lance_native_write";
    private static final String DATABASE = "default";

    @Override
    protected List<ConnectorTestConfig> connectorConfigs() {
        return List.of(ConnectorTestConfig.paimon(CATALOG, DATABASE, warehouse()));
    }

    private static String warehouse() {
        return tempDir.resolve("paimon-lance-native-write-warehouse").toString();
    }

    @Test
    void publicSqlPhysicalAppendIsReadableBySdkWithDoublePrecisionAndNulls() throws Exception {
        String suffix = UUID.randomUUID().toString().replace("-", "");
        String source = "source_" + suffix;
        String target = "target_" + suffix;
        createSdkTables(source, target);
        executeDml("INSERT INTO " + name(target) + " SELECT id, label, embedding FROM " + name(source)
                + " WHERE id <= 2");
        assertSdkRows(target, 1, List.of(1L, 2L));
        executeDml("INSERT INTO " + name(target) + " SELECT id, label, embedding FROM " + name(source)
                + " WHERE id = 3");
        assertSdkRows(target, 2, List.of(1L, 2L, 3L));
        var rows = executeQuery("SELECT id, label, embedding FROM " + name(target) + " ORDER BY id");
        assertThat(rows).hasSize(3);
        assertThat(rows.get(0).getRaw("label")).isEqualTo("café");
        assertThat(rows.get(0).getRaw("embedding")).isEqualTo(List.of(1.0000000001, -2.0, 3.5));
        assertThat(rows.get(1).getRaw("label")).isNull();
        assertThat(rows.get(1).getRaw("embedding")).isNull();
        assertThat(rows.get(2).getRaw("embedding")).isEqualTo(List.of(0.0, 4.0, -5.0));
    }

    @Test
    void genuineZeroInputPublicSqlDoesNotPublishADataObjectOrInventRows() throws Exception {
        String suffix = UUID.randomUUID().toString().replace("-", "");
        String source = "empty_source_" + suffix;
        String target = "empty_target_" + suffix;
        createSdkTables(source, target);
        executeDml("INSERT INTO " + name(target) + " SELECT id, label, embedding FROM " + name(source)
                + " WHERE id < 0");
        assertThat(executeQuery("SELECT id FROM " + name(target))).isEmpty();
        try (var catalog = sdkCatalog()) {
            var table = (FileStoreTable) catalog.getTable(Identifier.create(DATABASE, target));
            assertThat(table.newReadBuilder().newScan().plan().splits()).isEmpty();
            // This fixture uses a local warehouse; inspect the exact SDK target,
            // not only manifest visibility, to reject an unreferenced data object.
            try (var files = Files.walk(Path.of(table.location().toString()))) {
                assertThat(files.filter(Files::isRegularFile)
                        .filter(path -> path.getFileName().toString().endsWith(".lance")).toList()).isEmpty();
            }
        }
    }

    private static String name(String table) { return CATALOG + "." + DATABASE + "." + table; }

    private static org.apache.paimon.catalog.Catalog sdkCatalog() {
        Options options = new Options();
        options.setString("warehouse", warehouse());
        return CatalogFactory.createCatalog(CatalogContext.create(options));
    }

    private static Schema schema() {
        return Schema.newBuilder().column("id", DataTypes.BIGINT())
                .column("label", DataTypes.STRING()).column("embedding", DataTypes.VECTOR(3, DataTypes.DOUBLE()))
                .option("bucket", "1").option("bucket-key", "id").option("file.format", "lance")
                .option("lance.native.encoder-cache-bytes", "65536").build();
    }

    private static void createSdkTables(String source, String target) throws Exception {
        try (var catalog = sdkCatalog()) {
            catalog.createDatabase(DATABASE, true);
            var sourceId = Identifier.create(DATABASE, source);
            catalog.createTable(sourceId, schema(), false);
            catalog.createTable(Identifier.create(DATABASE, target), schema(), false);
            var table = (FileStoreTable) catalog.getTable(sourceId);
            var builder = table.newBatchWriteBuilder();
            try (var writer = builder.newWrite(); var commit = builder.newCommit()) {
                writer.write(GenericRow.of(1L, BinaryString.fromString("café"),
                        BinaryVector.fromPrimitiveArray(new double[] {1.0000000001, -2, 3.5})));
                writer.write(GenericRow.of(2L, null, null));
                writer.write(GenericRow.of(3L, BinaryString.fromString("last"),
                        BinaryVector.fromPrimitiveArray(new double[] {0, 4, -5})));
                commit.commit(writer.prepareCommit());
            }
            assertThat(table.snapshotManager().latestSnapshotId()).isEqualTo(1);
        }
    }

    private static void assertSdkRows(String target, long snapshot, List<Long> expectedIds) throws Exception {
        try (var catalog = sdkCatalog()) {
            var table = (FileStoreTable) catalog.getTable(Identifier.create(DATABASE, target));
            assertThat(table.snapshotManager().latestSnapshotId()).isEqualTo(snapshot);
            var read = table.newReadBuilder();
            var scan = read.newScan().plan();
            var files = scan.splits().stream().map(DataSplit.class::cast)
                    .flatMap(split -> split.dataFiles().stream()).toList();
            assertThat(files).hasSize((int) snapshot).allSatisfy(file -> {
                assertThat(file.fileName()).contains(".attempt-").endsWith(".lance");
                assertThat(file.fileFormat()).isEqualTo("lance");
                assertThat(file.fileSize()).isPositive();
                assertThat(file.schemaId()).isEqualTo(table.schema().id());
            });
            assertThat(files.stream().mapToLong(file -> file.rowCount()).sum()).isEqualTo(expectedIds.size());
            assertThat(files.stream().mapToLong(file -> file.maxSequenceNumber()).max().orElseThrow())
                    .isEqualTo(expectedIds.size() - 1L);
            var actual = new ArrayList<SdkRow>();
            try (var rows = read.newRead().createReader(scan).toCloseableIterator()) {
                while (rows.hasNext()) {
                    var row = rows.next();
                    actual.add(new SdkRow(row.getLong(0), row.isNullAt(1) ? null : row.getString(1).toString(),
                            row.isNullAt(2) ? null : row.getVector(2).toDoubleArray()));
                }
            }
            actual.sort(Comparator.comparingLong(SdkRow::id));
            assertThat(actual).extracting(SdkRow::id).containsExactlyElementsOf(expectedIds);
            assertThat(actual.get(0).label()).isEqualTo("café");
            assertThat(actual.get(0).embedding()).containsExactly(1.0000000001, -2, 3.5);
            assertThat(actual.get(1).label()).isNull();
            assertThat(actual.get(1).embedding()).isNull();
            if (actual.size() == 3) assertThat(actual.get(2).embedding()).containsExactly(0, 4, -5);
        }
    }

    private record SdkRow(long id, String label, double[] embedding) {}
}
