package org.pangrova.testing.integration.e2e.connector;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.hadoop.HadoopInputFile;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.parquet.GenericParquetReaders;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.parquet.Parquet;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.pangrova.types.value.TypedRow;
import org.pangrova.testing.integration.e2e.SqlE2ETestBase;
import org.pangrova.testing.integration.e2e.config.ConnectorTestConfig;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Iceberg TIMESTAMP_NS precision E2E tests: table creation, nanosecond-precision
 * insert/select, and ORDER BY correctness for values differing only in nanoseconds.
 *
 * <p>Verifies that nanosecond precision is preserved through the full pipeline:
 * TestCluster HTTP → NativeRuntime → Velox → Iceberg → local filesystem.
 * TIMESTAMP_NS is an Iceberg v3 feature requiring format-version=3.
 *
 * <p>Requirements: A7
 */
@DisplayName("Iceberg TIMESTAMP_NS Precision E2E Tests")
class IcebergTimestampNsTest extends SqlE2ETestBase {

    private static final String CATALOG = "iceberg_catalog";
    private static final String SCHEMA = "default";
    private static final String FQN_PREFIX = CATALOG + "." + SCHEMA + ".";

    @Override
    protected List<ConnectorTestConfig> connectorConfigs() {
        return List.of(
                ConnectorTestConfig.icebergLocal(CATALOG, SCHEMA, icebergWarehouse().toString()));
    }

    private String tableName;

    @BeforeEach
    void assignUniqueTableName(TestInfo testInfo) {
        String method = testInfo.getTestMethod().map(m -> m.getName()).orElse("unknown");
        tableName = "t_" + method + "_" + System.nanoTime();
    }

    private String fqn() {
        return FQN_PREFIX + tableName;
    }

    @Test
    @DisplayName("Timestamp literals retain fractional precision before storage")
    void timestampLiteralPreservesPrecisionBeforeWrite() {
        var rows = sqlRunner.execute("SELECT TIMESTAMP '2025-01-01 00:00:00.123456789' AS event_time");
        assertThat(rows).extracting(row -> row.getTimestamp("event_time"))
                .containsExactly(LocalDateTime.parse("2025-01-01T00:00:00.123456789"));
    }

    @Test
    @DisplayName("CTAS preserves nanos and creates a suitable v3 destination without explicit properties")
    void timestampNsCtasPreservesPrecision() {
        sqlRunner.executeDdl("CREATE TABLE " + fqn() + " (id BIGINT, event_time TIMESTAMP_NS)");
        sqlRunner.execute("INSERT INTO " + fqn() + " VALUES "
                + "(1, TIMESTAMP '2025-01-01 00:00:00.123456789'), "
                + "(2, TIMESTAMP '2025-01-01 00:00:00.123456788'), "
                + "(3, TIMESTAMP '1969-12-31 23:59:59.999999999')");
        String destination = fqn() + "_ctas";
        sqlRunner.executeDdl("CREATE TABLE " + destination + " AS SELECT id, event_time FROM " + fqn());
        var rows = sqlRunner.execute("SELECT event_time FROM " + destination + " ORDER BY id");
        assertThat(rows).extracting(row -> row.getTimestamp("event_time"))
                .containsExactly(LocalDateTime.parse("2025-01-01T00:00:00.123456789"),
                        LocalDateTime.parse("2025-01-01T00:00:00.123456788"),
                        LocalDateTime.parse("1969-12-31T23:59:59.999999999"));
        assertThat(sqlRunner.execute("SELECT id FROM " + destination
                + " WHERE event_time = TIMESTAMP '2025-01-01 00:00:00.123456789'"))
                .extracting(row -> row.getLong("id")).containsExactly(1L);
    }

    @Test
    @DisplayName("Create v3 table with TIMESTAMP_NS column and verify schema")
    void createTableWithTimestampNs() {
        sqlRunner.executeDdl("CREATE TABLE " + fqn()
                + " (id BIGINT, event_time TIMESTAMP_NS, label VARCHAR)"
                + " TBLPROPERTIES ('format-version' = '3')");

        List<TypedRow> rows = sqlRunner.execute(
                "SELECT id, event_time, label FROM " + fqn());

        // Table created successfully with TIMESTAMP_NS column; empty result expected
        assertThat(rows).isEmpty();
    }

    @Test
    @DisplayName("Insert timestamps with varying precision and verify at least millisecond fidelity")
    void insertAndSelectTimestampNs() {
        sqlRunner.executeDdl("CREATE TABLE " + fqn()
                + " (id BIGINT, event_time TIMESTAMP_NS)"
                + " TBLPROPERTIES ('format-version' = '3')");

        sqlRunner.execute("INSERT INTO " + fqn()
                + " (id, event_time) VALUES "
                + "(1, TIMESTAMP '2024-03-15 10:30:00.100'), "
                + "(2, TIMESTAMP '2024-03-15 10:30:00.200'), "
                + "(3, TIMESTAMP '2024-03-15 10:30:00.300')");

        List<TypedRow> rows = sqlRunner.execute(
                "SELECT id, event_time FROM " + fqn() + " ORDER BY id");

        assertThat(rows).hasSize(3);

        LocalDateTime ts1 = rows.get(0).getTimestamp("event_time");
        LocalDateTime ts2 = rows.get(1).getTimestamp("event_time");
        LocalDateTime ts3 = rows.get(2).getTimestamp("event_time");

        assertThat(ts1).isNotEqualTo(ts2);
        assertThat(ts1).isNotEqualTo(ts3);
        assertThat(ts2).isNotEqualTo(ts3);
    }

    @Test
    @DisplayName("ORDER BY correctly distinguishes timestamps differing in milliseconds")
    void timestampNsOrderBy() {
        sqlRunner.executeDdl("CREATE TABLE " + fqn()
                + " (id BIGINT, event_time TIMESTAMP_NS)"
                + " TBLPROPERTIES ('format-version' = '3')");

        sqlRunner.execute("INSERT INTO " + fqn()
                + " (id, event_time) VALUES "
                + "(1, TIMESTAMP '2024-06-01 12:00:00.300'), "
                + "(2, TIMESTAMP '2024-06-01 12:00:00.100'), "
                + "(3, TIMESTAMP '2024-06-01 12:00:00.200')");

        List<TypedRow> rows = sqlRunner.execute(
                "SELECT id, event_time FROM " + fqn() + " ORDER BY event_time ASC");

        assertThat(rows).hasSize(3);

        assertThat(rows.get(0).getLong("id")).isEqualTo(2L);
        assertThat(rows.get(1).getLong("id")).isEqualTo(3L);
        assertThat(rows.get(2).getLong("id")).isEqualTo(1L);
    }

    @Test
    @DisplayName("VALUES writes retain epoch sign and nanosecond fractions")
    void timestampValuesPreserveEpochAndFraction() throws Exception {
        sqlRunner.executeDdl("CREATE TABLE " + fqn()
                + " (id BIGINT, event_time TIMESTAMP_NS)"
                + " TBLPROPERTIES ('format-version' = '3')");
        sqlRunner.execute("INSERT INTO " + fqn() + " VALUES "
                + "(1, TIMESTAMP '2025-01-01 00:00:00.123456789'), "
                + "(2, TIMESTAMP '1969-12-31 23:59:59.999999999'), "
                + "(3, TIMESTAMP '1970-01-01 00:00:00'), "
                + "(4, TIMESTAMP '2025-01-01 00:00:00.123456788')");

        assertThat(readStoredTimestamps()).containsEntry(1L,
                LocalDateTime.parse("2025-01-01T00:00:00.123456789"))
                .containsEntry(4L, LocalDateTime.parse("2025-01-01T00:00:00.123456788"));

        var idOrdered = sqlRunner.execute("SELECT id, event_time FROM " + fqn() + " ORDER BY id");
        assertThat(idOrdered).extracting(row -> row.getTimestamp("event_time"))
                .containsExactly(LocalDateTime.parse("2025-01-01T00:00:00.123456789"),
                        LocalDateTime.parse("1969-12-31T23:59:59.999999999"),
                        LocalDateTime.parse("1970-01-01T00:00:00"),
                        LocalDateTime.parse("2025-01-01T00:00:00.123456788"));

        List<TypedRow> rows = sqlRunner.execute(
                "SELECT id, event_time FROM " + fqn() + " ORDER BY event_time");

        assertThat(rows).extracting(row -> row.getLong("id")).containsExactly(2L, 3L, 4L, 1L);
        assertThat(rows).extracting(row -> row.getTimestamp("event_time"))
                .containsExactly(
                        LocalDateTime.parse("1969-12-31T23:59:59.999999999"),
                        LocalDateTime.parse("1970-01-01T00:00:00"),
                        LocalDateTime.parse("2025-01-01T00:00:00.123456788"),
                        LocalDateTime.parse("2025-01-01T00:00:00.123456789"));
    }

    private Map<Long, LocalDateTime> readStoredTimestamps() throws Exception {
        Path tablePath = icebergWarehouse().resolve(SCHEMA).resolve(tableName);
        var schema = new HadoopTables(new Configuration()).load(tablePath.toString()).schema();
        Map<Long, LocalDateTime> values = new HashMap<>();
        try (var files = Files.list(tablePath.resolve("data"))) {
            for (Path path : files.filter(file -> file.toString().endsWith(".parquet")).toList()) {
                var input = HadoopInputFile.fromPath(new org.apache.hadoop.fs.Path(path.toUri()),
                        new Configuration());
                try (CloseableIterable<Record> rows = Parquet.read(input).project(schema)
                        .createReaderFunc(fileSchema -> GenericParquetReaders.buildReader(schema, fileSchema))
                        .build()) {
                    for (Record row : rows) {
                        values.put(((Number) row.getField("id")).longValue(),
                                (LocalDateTime) row.getField("event_time"));
                    }
                }
            }
        }
        return values;
    }

    @Test
    @DisplayName("Ordinary TIMESTAMP VALUES writes retain microsecond fractions")
    void timestampMicrosecondsPreserveEpochAndFraction() {
        sqlRunner.executeDdl("CREATE TABLE " + fqn()
                + " (id BIGINT, event_time TIMESTAMP)");
        sqlRunner.execute("INSERT INTO " + fqn() + " VALUES "
                + "(1, TIMESTAMP '1969-12-31 23:59:59.999999'), "
                + "(2, TIMESTAMP '1970-01-01 00:00:00.000001'), "
                + "(3, TIMESTAMP '2025-01-01 00:00:00.123456')");

        List<TypedRow> rows = sqlRunner.execute(
                "SELECT event_time FROM " + fqn() + " ORDER BY id");

        assertThat(rows).extracting(row -> row.getTimestamp("event_time"))
                .containsExactly(
                        LocalDateTime.parse("1969-12-31T23:59:59.999999"),
                        LocalDateTime.parse("1970-01-01T00:00:00.000001"),
                        LocalDateTime.parse("2025-01-01T00:00:00.123456"));
    }
}
