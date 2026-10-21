package org.pangrova.testing.e2e.connector;

import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.CatalogFactory;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.Decimal;
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

import java.math.BigDecimal;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.time.LocalDate;
import java.time.LocalDateTime;
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

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void scalarStatisticsMatchSdkAtStorageBoundaries(boolean primaryKey) throws Exception {
        String tableName = "file_scalar_stats_" + UUID.randomUUID().toString().replace("-", "");
        Options options = new Options();
        options.setString("warehouse", paimonWarehouse().toString());
        try (var catalog = CatalogFactory.createCatalog(CatalogContext.create(options))) {
            catalog.createDatabase("default", true);
            var schema = scalarStatisticsSchema().partitionKeys("region")
                    .option("bucket", "1").option("bucket-key", "id").option("file.format", "parquet");
            if (primaryKey) schema.primaryKey("region", "id");
            var identifier = Identifier.create("default", tableName);
            catalog.createTable(identifier, schema.build(), false);
            var source = (FileStoreTable) catalog.getTable(identifier);
            var nulls = new GenericRow(source.rowType().getFieldCount());
            nulls.setField(0, 3L);
            nulls.setField(1, BinaryString.fromString("east"));
            write(source, scalarStatisticsRow(false), scalarStatisticsRow(true), nulls);
            var expected = sdkFiles(source);
            assertThat(expected).isNotEmpty().allSatisfy(row -> {
                assertThat(row).hasSize(20);
                assertThat(row.get("null_value_counts").toString()).contains("small_decimal=1");
                assertThat(row.get("min_value_stats").toString()).contains("min_float=");
            });
            assertThat(executeQuery("SELECT * FROM " + CATALOG + ".default." + tableName + "$files"))
                    .extracting(TypedRow::toRawMap).containsExactlyInAnyOrderElementsOf(expected);
        }
    }

    private static Schema.Builder scalarStatisticsSchema() {
        return Schema.newBuilder().column("id", DataTypes.BIGINT()).column("region", DataTypes.STRING())
                .column("flag", DataTypes.BOOLEAN()).column("tiny", DataTypes.TINYINT())
                .column("small", DataTypes.SMALLINT()).column("whole", DataTypes.INT())
                .column("large", DataTypes.BIGINT()).column("min_float", DataTypes.FLOAT())
                .column("min_double", DataTypes.DOUBLE()).column("fixed_text", DataTypes.CHAR(6))
                .column("text", DataTypes.VARCHAR(32)).column("fixed_bytes", DataTypes.BINARY(4))
                .column("bytes", DataTypes.VARBINARY(16)).column("small_decimal", DataTypes.DECIMAL(18, 4))
                .column("large_decimal", DataTypes.DECIMAL(38, 12)).column("day", DataTypes.DATE())
                .column("clock_s", DataTypes.TIME(0)).column("clock_ms", DataTypes.TIME(3))
                .column("stamp_s", DataTypes.TIMESTAMP(0)).column("stamp_ms", DataTypes.TIMESTAMP(3))
                .column("stamp_us", DataTypes.TIMESTAMP(6)).column("stamp_ns", DataTypes.TIMESTAMP(9))
                .column("stamp_ltz", DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(6));
    }

    private static GenericRow scalarStatisticsRow(boolean high) {
        return GenericRow.of(high ? 2L : 1L, BinaryString.fromString("east"), high,
                high ? Byte.MAX_VALUE : Byte.MIN_VALUE, high ? Short.MAX_VALUE : Short.MIN_VALUE,
                high ? Integer.MAX_VALUE : Integer.MIN_VALUE, high ? Long.MAX_VALUE : Long.MIN_VALUE,
                high ? Float.MIN_VALUE : -Float.MAX_VALUE, high ? Double.MIN_VALUE : -Double.MAX_VALUE,
                BinaryString.fromString(high ? "z   " : "café"), BinaryString.fromString(high ? "東京" : "a,=b"),
                high ? new byte[] {127, -1, 0, 1} : new byte[] {0, 1, -128, -1},
                high ? new byte[] {-1, 0} : new byte[] {0},
                Decimal.fromBigDecimal(new BigDecimal(high ? "99999999999999.9999" : "-99999999999999.9999"), 18, 4),
                Decimal.fromBigDecimal(new BigDecimal(high ? "0.000000000001"
                        : "-99999999999999999999999999.999999999999"), 38, 12),
                Math.toIntExact((high ? LocalDate.of(9999, 12, 31) : LocalDate.of(1, 1, 1)).toEpochDay()),
                high ? 86_399_000 : 0, high ? 86_399_999 : 0,
                Timestamp.fromLocalDateTime(LocalDateTime.parse(high ? "2038-01-19T03:14:07" : "1969-12-31T23:59:59")),
                Timestamp.fromLocalDateTime(LocalDateTime.parse(
                        high ? "2038-01-19T03:14:07.999" : "1969-12-31T23:59:59.123")),
                Timestamp.fromLocalDateTime(LocalDateTime.parse(
                        high ? "2038-01-19T03:14:07.999999" : "1969-12-31T23:59:59.123456")),
                Timestamp.fromLocalDateTime(LocalDateTime.parse(
                        high ? "2038-01-19T03:14:07.999999999" : "1969-12-31T23:59:59.123456789")),
                Timestamp.fromLocalDateTime(LocalDateTime.parse(
                        high ? "2038-01-19T03:14:07.999999" : "1969-12-31T23:59:59.123456")));
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
