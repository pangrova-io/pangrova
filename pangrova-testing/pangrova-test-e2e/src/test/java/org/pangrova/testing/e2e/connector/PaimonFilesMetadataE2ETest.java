package org.pangrova.testing.e2e.connector;

import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.CatalogFactory;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalArray;
import org.apache.paimon.data.Timestamp;
import org.apache.paimon.options.Options;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.SchemaChange;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.sink.BatchWriteBuilder;
import org.apache.paimon.table.system.FilesTable;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.utils.RowDataToObjectArrayConverter;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.pangrova.spi.connector.data.TypedRow;
import org.pangrova.testing.E2ETest;
import org.pangrova.testing.e2e.PangrovaE2ETestBase;
import org.pangrova.testing.e2e.config.ConnectorTestConfig;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

@E2ETest
class PaimonFilesMetadataE2ETest extends PangrovaE2ETestBase {
    private static final String CATALOG = "paimon_files_metadata";

    @Override
    protected List<ConnectorTestConfig> connectorConfigs() {
        return List.of(ConnectorTestConfig.paimon(CATALOG, "default", paimonWarehouse().toString()));
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void allNativeFileColumnsMatchThePinnedSdkAcrossCommits(boolean primaryKey) throws Exception {
        String tableName = "file_columns_" + UUID.randomUUID().toString().replace("-", "");
        Options options = new Options();
        options.setString("warehouse", paimonWarehouse().toString());
        try (var catalog = CatalogFactory.createCatalog(CatalogContext.create(options))) {
            catalog.createDatabase("default", true);
            var schema = Schema.newBuilder().column("id", DataTypes.BIGINT())
                    .column("region", DataTypes.STRING()).column("label", DataTypes.STRING())
                    .partitionKeys("region").option("bucket", "1").option("bucket-key", "id")
                    .option("file.format", "parquet");
            if (primaryKey) schema.primaryKey("region", "id");
            var id = Identifier.create("default", tableName);
            catalog.createTable(id, schema.build(), false);
            var source = (FileStoreTable) catalog.getTable(id);
            write(source, GenericRow.of(1L, BinaryString.fromString("east"), BinaryString.fromString("old")),
                    GenericRow.of(3L, BinaryString.fromString("east"), BinaryString.fromString("café")),
                    GenericRow.of(2L, BinaryString.fromString("west"), null));
            write(source, GenericRow.of(1L, BinaryString.fromString("east"), BinaryString.fromString("new")));
            var expected = sdkFiles(source);
            assertThat(expected).isNotEmpty();
            assertThat(expected.get(0)).hasSize(20);
            capture(source, expected, primaryKey);
            var actual = executeQuery("SELECT * FROM " + CATALOG + ".default." + tableName + "$files");
            assertThat(actual).extracting(TypedRow::toRawMap).containsExactlyInAnyOrderElementsOf(expected);
            assertThat(executeQuery("SELECT SUM(record_count) AS total FROM " + CATALOG
                    + ".default." + tableName + "$files"))
                    .singleElement().satisfies(row -> assertThat(row.getRaw("total"))
                            .isEqualTo(expected.stream().mapToLong(value -> (Long) value.get("record_count")).sum()));
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void emptyHistoricalAndEvolvedMetadataUseTheSameNativeReader(boolean primaryKey) throws Exception {
        String tableName = "file_evolution_" + UUID.randomUUID().toString().replace("-", "");
        String qualified = CATALOG + ".default." + tableName;
        Options options = new Options();
        options.setString("warehouse", paimonWarehouse().toString());
        try (var catalog = CatalogFactory.createCatalog(CatalogContext.create(options))) {
            catalog.createDatabase("default", true);
            var schema = Schema.newBuilder().column("id", DataTypes.BIGINT())
                    .column("region", DataTypes.STRING()).column("label", DataTypes.STRING())
                    .partitionKeys("region").option("bucket", "1").option("bucket-key", "id")
                    .option("file.format", "parquet");
            if (primaryKey) schema.primaryKey("region", "id");
            var id = Identifier.create("default", tableName);
            catalog.createTable(id, schema.build(), false);
            assertThat(executeQuery("SELECT * FROM " + qualified + "$files")).isEmpty();
            assertThat(executeQuery("SELECT COUNT(*) AS n FROM " + qualified + "$files"))
                    .singleElement().satisfies(row -> assertThat(row.getRaw("n")).isEqualTo(0L));
            var source = (FileStoreTable) catalog.getTable(id);
            write(source, GenericRow.of(1L, BinaryString.fromString("east"), BinaryString.fromString("original")));
            var first = sdkFiles(source);
            write(source, GenericRow.of(2L, BinaryString.fromString("west"), null));
            assertThat(executeQuery("SELECT * FROM " + qualified + "$files FOR VERSION AS OF 1"))
                    .extracting(TypedRow::toRawMap).containsExactlyInAnyOrderElementsOf(first);
            catalog.alterTable(id, List.of(SchemaChange.renameColumn("label", "renamed"),
                    SchemaChange.addColumn("added", DataTypes.BIGINT())), false);
            var evolved = (FileStoreTable) catalog.getTable(id);
            var expected = sdkFiles(evolved);
            assertThat(executeQuery("SELECT * FROM " + qualified + "$files"))
                    .extracting(TypedRow::toRawMap).containsExactlyInAnyOrderElementsOf(expected);
            assertThat(executeQuery("SHOW FILES FROM " + qualified))
                    .extracting(TypedRow::toRawMap).containsExactlyInAnyOrderElementsOf(expected);
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void overwriteExcludesDeletedFilesWhileKeepingUnchangedManifestEntries(boolean primaryKey) throws Exception {
        String tableName = "file_overwrite_" + UUID.randomUUID().toString().replace("-", "");
        String qualified = CATALOG + ".default." + tableName;
        Options options = new Options();
        options.setString("warehouse", paimonWarehouse().toString());
        try (var catalog = CatalogFactory.createCatalog(CatalogContext.create(options))) {
            catalog.createDatabase("default", true);
            var schema = Schema.newBuilder().column("id", DataTypes.BIGINT())
                    .column("region", DataTypes.STRING()).column("label", DataTypes.STRING())
                    .partitionKeys("region").option("bucket", "1").option("bucket-key", "id")
                    .option("file.format", "parquet");
            if (primaryKey) schema.primaryKey("region", "id");
            var id = Identifier.create("default", tableName);
            catalog.createTable(id, schema.build(), false);
            var source = (FileStoreTable) catalog.getTable(id);
            write(source, GenericRow.of(1L, BinaryString.fromString("east"), BinaryString.fromString("old")),
                    GenericRow.of(2L, BinaryString.fromString("west"), BinaryString.fromString("keep")));
            var before = sdkFiles(source);
            var deleted = before.stream().filter(row -> row.get("partition").equals("{east}"))
                    .findFirst().orElseThrow().get("file_path");
            write(source.newBatchWriteBuilder().withOverwrite(Map.of("region", "east")),
                    GenericRow.of(3L, BinaryString.fromString("east"), BinaryString.fromString("replacement")));
            var expected = sdkFiles(source);
            assertThat(expected).hasSize(2);
            assertThat(expected).extracting(row -> row.get("file_path")).doesNotContain(deleted);
            assertThat(executeQuery("SELECT * FROM " + qualified + "$files"))
                    .extracting(TypedRow::toRawMap).containsExactlyInAnyOrderElementsOf(expected);
        }
    }

    private static void write(FileStoreTable table, GenericRow... rows) throws Exception {
        write(table.newBatchWriteBuilder(), rows);
    }

    private static void write(BatchWriteBuilder builder, GenericRow... rows) throws Exception {
        try (var writer = builder.newWrite(); var commit = builder.newCommit()) {
            for (var row : rows) writer.write(row);
            commit.commit(writer.prepareCommit());
        }
    }

    private static List<Map<String, Object>> sdkFiles(FileStoreTable table) throws Exception {
        var files = new FilesTable(table);
        var builder = files.newReadBuilder();
        var converter = new RowDataToObjectArrayConverter(files.rowType());
        List<Map<String, Object>> expected = new ArrayList<>();
        try (var rows = builder.newRead().createReader(builder.newScan().plan()).toCloseableIterator()) {
            while (rows.hasNext()) {
                Object[] values = converter.convert(rows.next());
                Map<String, Object> row = new LinkedHashMap<>();
                for (int index = 0; index < values.length; index++) {
                    row.put(files.rowType().getFieldNames().get(index), normalize(values[index]));
                }
                expected.add(row);
            }
        }
        return expected;
    }

    private static Object normalize(Object value) {
        if (value instanceof BinaryString text) return text.toString();
        if (value instanceof Timestamp timestamp) return timestamp.toLocalDateTime();
        if (value instanceof InternalArray array) {
            List<String> items = new ArrayList<>();
            for (int index = 0; index < array.size(); index++) {
                items.add(array.isNullAt(index) ? null : array.getString(index).toString());
            }
            return items;
        }
        return value;
    }

    private static void capture(FileStoreTable table, List<Map<String, Object>> expected, boolean primaryKey)
            throws Exception {
        String location = System.getProperty("pangrova.e2e.paimon.metadata-capture-dir");
        if (location == null || location.isBlank()) return;
        Path directory = Path.of(location).resolve(primaryKey ? "primary-key" : "append");
        Files.createDirectories(directory);
        var uri = table.location().toUri();
        Path source = uri.getScheme() == null ? Path.of(uri.getPath()) : Path.of(uri);
        for (String kind : List.of("manifest", "schema", "snapshot")) {
            Path metadata = source.resolve(kind);
            if (!Files.exists(metadata)) continue;
            try (var paths = Files.walk(metadata)) {
                for (Path path : paths.filter(Files::isRegularFile).toList()) {
                    Path target = directory.resolve(kind).resolve(metadata.relativize(path));
                    Files.createDirectories(target.getParent());
                    Files.copy(path, target, StandardCopyOption.REPLACE_EXISTING);
                }
            }
        }
        Files.writeString(directory.resolve("sdk-files.txt"), expected.toString());
    }
}
