package org.pangrova.testing.integration.e2e.connector;

import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.parquet.GenericParquetReaders;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.types.Types.UUIDType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.pangrova.types.value.ColumnValue;
import org.pangrova.testing.testkit.E2ETest;
import org.pangrova.testing.integration.e2e.PangrovaE2ETestBase;
import org.pangrova.testing.integration.e2e.config.ConnectorTestConfig;

import java.sql.DriverManager;
import java.sql.Types;
import java.time.Duration;
import java.util.HexFormat;
import java.util.List;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

@E2ETest
class UuidTypeE2ETest extends PangrovaE2ETestBase {
    private static final String CATALOG = "uuid_types";
    private static final String ZERO = "00000000-0000-0000-0000-000000000000";
    private static final String MIXED = "00112233-4455-6677-8899-aabbccddeeff";
    private static final String ALL = "ffffffff-ffff-ffff-ffff-ffffffffffff";

    @Override
    protected List<ConnectorTestConfig> connectorConfigs() {
        return List.of(ConnectorTestConfig.icebergLocal(CATALOG, "default", icebergWarehouse().toString()));
    }

    @Test
    void nativeUuidCastsPreserveCanonicalBinaryAndStringValues() {
        String hex = MIXED.replace("-", "");
        var rows = executeQuery("SELECT CAST(X'" + hex + "' AS UUID) AS id, CAST(UUID '" + MIXED
                + "' AS VARCHAR) AS text_id, CAST(UUID '" + MIXED + "' AS VARBINARY) AS binary_id");
        assertThat(rows).singleElement().satisfies(row -> {
            assertThat(row.getRaw("id")).isEqualTo(UUID.fromString(MIXED));
            assertThat(row.getRaw("text_id")).isEqualTo(MIXED);
            assertThat((byte[]) row.getRaw("binary_id")).containsExactly(HexFormat.of().parseHex(hex));
        });
    }

    @ParameterizedTest
    @ValueSource(ints = {0, 15, 17})
    void malformedUuidBinaryCastsFailAndReleaseTheQuery(int width) {
        assertThat(executeExpectingError("SELECT CAST(X'" + "00".repeat(width) + "' AS UUID) AS invalid"))
                .isNotBlank();
        await().atMost(Duration.ofSeconds(30)).untilAsserted(() -> {
            assertThat(queryManager.getActiveQueryIds()).isEmpty();
            assertThat(queryManager.getActiveQueryCount()).isZero();
        });
    }

    @ParameterizedTest(name = "UUID Parquet v{0}")
    @ValueSource(ints = {2, 3})
    void nativeUuidParquetWritesMatchTheIcebergSdk(int version) throws Exception {
        String name = "uuid_data_" + UUID.randomUUID().toString().replace("-", "");
        String table = CATALOG + ".default." + name;
        Map<Long, UUID> expected = new LinkedHashMap<>();
        expected.put(1L, UUID.fromString(ZERO));
        expected.put(2L, UUID.fromString(MIXED));
        expected.put(3L, UUID.fromString(ALL));
        expected.put(4L, null);
        try {
            executeDdl("CREATE TABLE " + table + " (row_id BIGINT, id UUID)"
                    + " TBLPROPERTIES ('format-version' = '" + version + "')");
            executeDml("INSERT INTO " + table + " VALUES (1, UUID '" + ZERO
                    + "'), (2, UUID '" + MIXED + "'), (3, UUID '" + ALL
                    + "'), (4, CAST(NULL AS UUID))");

            var sdk = new HadoopTables(new Configuration()).load(
                    icebergWarehouse().resolve("default").resolve(name).toString());
            assertThat(sdk.schema().findType("id")).isEqualTo(UUIDType.get());
            Map<Long, Object> stored = new LinkedHashMap<>();
            try (var files = sdk.newScan().planFiles()) {
                for (var file : files) {
                    try (CloseableIterable<Record> records = Parquet.read(
                            sdk.io().newInputFile(file.file().path().toString()))
                            .project(sdk.schema()).createReaderFunc(fileSchema ->
                                    GenericParquetReaders.buildReader(sdk.schema(), fileSchema)).build()) {
                        for (var row : records) {
                            assertThat(stored.containsKey((Long) row.getField("row_id"))).isFalse();
                            stored.put((Long) row.getField("row_id"), row.getField("id"));
                        }
                    }
                }
            }
            assertThat(stored).isEqualTo(expected);
            var rows = executeQuery("SELECT row_id, id FROM " + table + " ORDER BY row_id");
            assertThat(rows).hasSize(4);
            for (int index = 0; index < rows.size(); ++index) {
                assertThat(rows.get(index).getRaw("row_id")).isEqualTo((long) index + 1);
                assertThat(rows.get(index).getRaw("id")).isEqualTo(expected.get((long) index + 1));
            }
            assertThat(executeQuery("SELECT row_id FROM " + table + " WHERE id = UUID '" + MIXED + "'"))
                    .singleElement().satisfies(row -> assertThat(row.getRaw("row_id")).isEqualTo(2L));
            assertThat(executeQuery("SELECT row_id FROM " + table + " WHERE id < UUID '" + ALL
                    + "' ORDER BY row_id")).extracting(row -> row.getRaw("row_id")).containsExactly(1L, 2L);
            assertThat(executeQuery("SELECT row_id FROM " + table + " WHERE id > UUID '" + ZERO
                    + "' ORDER BY row_id")).extracting(row -> row.getRaw("row_id")).containsExactly(2L, 3L);
            assertThat(executeQuery("SELECT row_id FROM " + table + " WHERE id IN (UUID '" + ZERO
                    + "', UUID '" + ALL + "') ORDER BY row_id"))
                    .extracting(row -> row.getRaw("row_id")).containsExactly(1L, 3L);
            assertThat(executeQuery("SELECT row_id FROM " + table + " WHERE id IS NULL"))
                    .singleElement().satisfies(row -> assertThat(row.getRaw("row_id")).isEqualTo(4L));
        } finally {
            executeDdl("DROP TABLE IF EXISTS " + table);
        }
    }

    @Test
    void nativeUuidResultsRetainTheirIdentityAndMetadataThroughJdbc() throws Exception {
        Properties credentials = new Properties();
        credentials.setProperty("token", E2E_USER_BEARER_TOKEN);
        try (var connection = DriverManager.getConnection(
                "jdbc:pangrova://localhost:" + coordinatorHttpServer.getPort(), credentials);
             var statement = connection.createStatement();
             var result = statement.executeQuery("SELECT UUID '" + MIXED + "' AS id, CAST(NULL AS UUID) AS absent")) {
            var metadata = result.getMetaData();
            for (int column = 1; column <= 2; ++column) {
                assertThat(metadata.getColumnType(column)).isEqualTo(Types.OTHER);
                assertThat(metadata.getColumnTypeName(column)).isEqualTo("UUID");
                assertThat(metadata.getColumnClassName(column)).isEqualTo(UUID.class.getName());
                assertThat(metadata.getColumnDisplaySize(column)).isEqualTo(36);
                assertThat(metadata.getPrecision(column)).isZero();
            }
            assertThat(result.next()).isTrue();
            assertThat(result.getObject(1)).isEqualTo(UUID.fromString(MIXED));
            assertThat(result.getString(1)).isEqualTo(MIXED);
            assertThat(result.getObject(2)).isNull();
            assertThat(result.wasNull()).isTrue();
            assertThat(result.next()).isFalse();
        }
    }

    @Test
    void nativeNestedUuidAndGroupingSurviveTheFullResultPath() {
        var nested = executeQuery("SELECT ARRAY[UUID '" + ZERO + "', UUID '" + ALL
                + "', CAST(NULL AS UUID)] AS ids, MAP[UUID '" + MIXED + "', UUID '" + ALL + "'] AS mapping");
        assertThat(nested).hasSize(1);
        assertThat((List<?>) nested.get(0).getRaw("ids"))
                .isEqualTo(java.util.Arrays.asList(UUID.fromString(ZERO), UUID.fromString(ALL), null));
        assertThat(nested.get(0).getRaw("mapping"))
                .isEqualTo(Map.of(UUID.fromString(MIXED), UUID.fromString(ALL)));
        var grouped = executeQuery("SELECT id, COUNT(*) AS total FROM (VALUES (UUID '" + MIXED
                + "'), (UUID '" + MIXED + "'), (UUID '" + ALL + "'), (CAST(NULL AS UUID))) AS t(id) GROUP BY id");
        assertThat(grouped).hasSize(3);
        assertThat(grouped.stream().map(row -> row.getRaw("id")).toList())
                .containsExactlyInAnyOrder(UUID.fromString(MIXED), UUID.fromString(ALL), null);
        for (var row : grouped) {
            Object id = ColumnValue.toRaw(row.get("id"));
            assertThat(row.getRaw("total")).isEqualTo(UUID.fromString(MIXED).equals(id) ? 2L : 1L);
        }
    }

    @Test
    void nativeSortAndScalarUuidComparisonAgreeAcrossTheSignBoundary() {
        String input = " FROM (VALUES (UUID '" + ALL + "'), (UUID '" + MIXED
                + "'), (UUID '" + ZERO + "')) AS t(id)";
        var rows = executeQuery("SELECT id, id < UUID '" + ALL + "' AS smaller" + input + " ORDER BY id");
        assertThat(rows).hasSize(3);
        assertThat(rows.stream().map(row -> row.getRaw("id")).toList())
                .containsExactly(UUID.fromString(ZERO), UUID.fromString(MIXED), UUID.fromString(ALL));
        assertThat(rows.stream().map(row -> row.getRaw("smaller")).toList()).containsExactly(true, true, false);
        var extrema = executeQuery("SELECT MIN(id) AS lo, MAX(id) AS hi" + input);
        assertThat(extrema).hasSize(1);
        assertThat(extrema.get(0).getRaw("lo")).isEqualTo(UUID.fromString(ZERO));
        assertThat(extrema.get(0).getRaw("hi")).isEqualTo(UUID.fromString(ALL));
        var top = executeQuery("SELECT id" + input + " ORDER BY id DESC LIMIT 2");
        assertThat(top.stream().map(row -> row.getRaw("id")).toList())
                .containsExactly(UUID.fromString(ALL), UUID.fromString(MIXED));
    }
}
