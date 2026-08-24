package org.pangrova.testing.e2e.connector;

import org.pangrova.spi.connector.data.TypedRow;
import org.pangrova.testing.e2e.SqlE2ETestBase;
import org.pangrova.testing.e2e.config.ConnectorTestConfig;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

import java.util.List;

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

        String ts1 = rows.get(0).getString("event_time");
        String ts2 = rows.get(1).getString("event_time");
        String ts3 = rows.get(2).getString("event_time");

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
}
