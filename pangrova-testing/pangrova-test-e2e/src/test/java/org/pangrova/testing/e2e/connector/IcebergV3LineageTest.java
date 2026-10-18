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
 * Iceberg v3 row-level lineage E2E tests.
 *
 * <p>Verifies that v3 tables with row lineage enabled expose
 * {@code first_row_id} and {@code data_sequence_number} via the
 * {@code $entries} system table, and that v2 tables show null
 * for {@code first_row_id}.
 *
 * <p>Requirements: A6
 */
@DisplayName("Iceberg v3 Row-Level Lineage E2E Tests")
class IcebergV3LineageTest extends SqlE2ETestBase {

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

    private String fqn(String name) {
        return FQN_PREFIX + name;
    }

    @Test
    @DisplayName("v3 table entries expose sequence_number and data_sequence_number")
    void v3TableEntriesHaveFirstRowId() {
        sqlRunner.executeDdl("CREATE TABLE " + fqn()
                + " (id BIGINT, name VARCHAR)"
                + " TBLPROPERTIES ('format-version' = '3')");

        sqlRunner.execute("INSERT INTO " + fqn()
                + " (id, name) VALUES (1, 'alice'), (2, 'bob')");

        List<TypedRow> entries = sqlRunner.execute(
                "SELECT status, snapshot_id, sequence_number, file_path, data_sequence_number, first_row_id"
                + " FROM " + fqn() + "$entries");

        assertThat(entries).isNotEmpty();
        for (TypedRow entry : entries) {
            assertThat(entry.getInt("status")).isBetween(0, 1);
            assertThat(entry.getLong("data_sequence_number"))
                    .as("data_sequence_number should be populated")
                    .isNotNull();
            assertThat(entry.getLong("sequence_number"))
                    .as("sequence_number should be populated")
                    .isNotNull();
            assertThat(entry.getLong("first_row_id"))
                    .as("v3 first_row_id should be assigned")
                    .isNotNull();
        }
    }

    @Test
    @DisplayName("v3 table entries have populated data_sequence_number")
    void v3TableEntriesHaveDataSequenceNumber() {
        sqlRunner.executeDdl("CREATE TABLE " + fqn()
                + " (id BIGINT, value DOUBLE)"
                + " TBLPROPERTIES ('format-version' = '3')");

        sqlRunner.execute("INSERT INTO " + fqn()
                + " (id, value) VALUES (1, 10.0)");
        sqlRunner.execute("INSERT INTO " + fqn()
                + " (id, value) VALUES (2, 20.0)");

        List<TypedRow> entries = sqlRunner.execute(
                "SELECT data_sequence_number FROM " + fqn() + "$entries");

        assertThat(entries).hasSizeGreaterThanOrEqualTo(2);
        for (TypedRow entry : entries) {
            Long dsn = entry.getLong("data_sequence_number");
            assertThat(dsn)
                    .as("data_sequence_number should be populated for each data file")
                    .isNotNull();
            assertThat(dsn)
                    .as("data_sequence_number should be positive")
                    .isGreaterThan(0L);
        }
    }

    @Test
    @DisplayName("incremental query using data_sequence_number to identify new files")
    void incrementalQueryUsingSequenceNumber() {
        sqlRunner.executeDdl("CREATE TABLE " + fqn()
                + " (id BIGINT, name VARCHAR)"
                + " TBLPROPERTIES ('format-version' = '3')");

        sqlRunner.execute("INSERT INTO " + fqn()
                + " (id, name) VALUES (1, 'alice'), (2, 'bob')");

        List<TypedRow> baselineEntries = sqlRunner.execute(
                "SELECT data_sequence_number FROM " + fqn() + "$entries");
        long baselineDsn = baselineEntries.stream()
                .mapToLong(r -> r.getLong("data_sequence_number"))
                .max()
                .orElse(0L);

        sqlRunner.execute("INSERT INTO " + fqn()
                + " (id, name) VALUES (3, 'charlie'), (4, 'diana')");

        List<TypedRow> allEntries = sqlRunner.execute(
                "SELECT data_sequence_number, file_path FROM " + fqn() + "$entries");

        List<TypedRow> incremental = allEntries.stream()
                .filter(r -> r.getLong("data_sequence_number") > baselineDsn)
                .toList();

        assertThat(incremental).isNotEmpty();
        for (TypedRow entry : incremental) {
            long dsn = entry.getLong("data_sequence_number");
            assertThat(dsn)
                    .as("incremental entries should have dsn > baseline %d", baselineDsn)
                    .isGreaterThan(baselineDsn);
        }
    }

    @Test
    @DisplayName("v2 table entries have populated sequence_number")
    void v2TableEntriesHaveNullFirstRowId() {
        sqlRunner.executeDdl("CREATE TABLE " + fqn()
                + " (id BIGINT, name VARCHAR)");

        sqlRunner.execute("INSERT INTO " + fqn()
                + " (id, name) VALUES (1, 'alice'), (2, 'bob')");

        List<TypedRow> entries = sqlRunner.execute(
                "SELECT sequence_number, data_sequence_number, first_row_id FROM " + fqn() + "$entries");

        assertThat(entries).isNotEmpty();
        for (TypedRow entry : entries) {
            assertThat(entry.getLong("sequence_number"))
                    .as("v2 table should have populated sequence_number")
                    .isNotNull();
            assertThat(entry.getLong("first_row_id"))
                    .as("v2 table must not synthesize v3 row IDs")
                    .isNull();
        }
    }
}
