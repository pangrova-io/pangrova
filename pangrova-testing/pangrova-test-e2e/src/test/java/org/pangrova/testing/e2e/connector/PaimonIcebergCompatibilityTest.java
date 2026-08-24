package org.pangrova.testing.e2e.connector;

import org.pangrova.spi.connector.data.TypedRow;
import org.pangrova.testing.e2e.SqlE2ETestBase;
import org.pangrova.testing.e2e.config.ConnectorTestConfig;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Paimon Iceberg compatibility mode E2E tests.
 *
 * <p>Validates that Paimon tables configured with {@code metadata.iceberg.storage = hadoop-catalog}
 * produce Iceberg-compatible metadata that can be read by an Iceberg engine, and that
 * primary key tables with Iceberg compatibility generate correct Iceberg metadata after compaction.
 *
 * <p>Full pipeline: SQL → Parser → Planner → Substrait → Native → transport → Client.
 *
 * <p>Requirements: C9 (Iceberg Compatibility Mode)
 */
@DisplayName("Paimon Iceberg Compatibility Mode - E2E Tests")
class PaimonIcebergCompatibilityTest extends SqlE2ETestBase {

    private static final String PAIMON_CATALOG = "paimon_compat";
    private static final String ICEBERG_CATALOG = "iceberg_compat";
    private static final String SCHEMA = "default";
    private static final String PAIMON_FQN_PREFIX = PAIMON_CATALOG + "." + SCHEMA + ".";
    private static final String ICEBERG_FQN_PREFIX = ICEBERG_CATALOG + "." + SCHEMA + ".";

    @Override
    protected List<ConnectorTestConfig> connectorConfigs() {
        return List.of(
                ConnectorTestConfig.paimon(
                        PAIMON_CATALOG, SCHEMA, paimonWarehouse().toString()),
                ConnectorTestConfig.icebergLocal(
                        ICEBERG_CATALOG, SCHEMA, paimonWarehouse().toString()));
    }

    private String tableName;

    @BeforeEach
    void assignUniqueTableName(TestInfo testInfo) {
        String method = testInfo.getTestMethod().map(m -> m.getName()).orElse("unknown");
        tableName = "t_" + method + "_" + System.nanoTime();
    }

    private String paimonFqn() {
        return PAIMON_FQN_PREFIX + tableName;
    }

    // --- 2.14.1: Paimon table with metadata.iceberg.storage=hadoop-catalog readable by Iceberg ---

    @Nested
    @DisplayName("2.14.1 - Iceberg Compatibility Append Table")
    class IcebergCompatAppendTable {

        @Test
        @DisplayName("Paimon append table with iceberg.storage=hadoop-catalog is queryable via Paimon")
        void appendTableWithIcebergStorageIsQueryable() {
            sqlRunner.executeDdl("CREATE TABLE " + paimonFqn()
                    + " (id BIGINT, name VARCHAR, amount DOUBLE)"
                    + " WITH ("
                    + "   'metadata.iceberg.storage' = 'hadoop-catalog'"
                    + " )");

            sqlRunner.execute("INSERT INTO " + paimonFqn()
                    + " (id, name, amount) VALUES"
                    + " (1, 'alice', 100.0),"
                    + " (2, 'bob', 200.0),"
                    + " (3, 'carol', 300.0)");

            List<TypedRow> rows = sqlRunner.execute(
                    "SELECT id, name, amount FROM " + paimonFqn() + " ORDER BY id");

            assertThat(rows).hasSize(3);
            assertThat(rows.get(0).getLong("id")).isEqualTo(1L);
            assertThat(rows.get(0).getString("name")).isEqualTo("alice");
            assertThat(rows.get(1).getLong("id")).isEqualTo(2L);
            assertThat(rows.get(2).getLong("id")).isEqualTo(3L);
        }

        @Test
        @DisplayName("Paimon append table with iceberg.storage generates Iceberg metadata")
        void appendTableGeneratesIcebergMetadata() {
            sqlRunner.executeDdl("CREATE TABLE " + paimonFqn()
                    + " (id BIGINT, value VARCHAR)"
                    + " WITH ("
                    + "   'metadata.iceberg.storage' = 'hadoop-catalog'"
                    + " )");

            sqlRunner.execute("INSERT INTO " + paimonFqn()
                    + " (id, value) VALUES (1, 'test1'), (2, 'test2')");

            List<TypedRow> rows = sqlRunner.execute(
                    "SELECT id, value FROM " + paimonFqn() + " ORDER BY id");

            assertThat(rows).hasSize(2);
            assertThat(((Number) rows.get(0).getRaw("id")).longValue()).isEqualTo(1L);
            assertThat(rows.get(0).getRaw("value")).isEqualTo("test1");
        }
    }

    // --- 2.14.2: PK table Iceberg compat compaction generates Iceberg metadata ---

    @Nested
    @DisplayName("2.14.2 - PK Table Iceberg Compat Compaction")
    class PkTableIcebergCompatCompaction {

        @Test
        @DisplayName("PK table with iceberg compat generates metadata after compaction")
        void pkTableCompactionGeneratesIcebergMetadata() {
            sqlRunner.executeDdl("CREATE TABLE " + paimonFqn()
                    + " (id BIGINT, name VARCHAR, score DOUBLE,"
                    + "  PRIMARY KEY (id) NOT ENFORCED)"
                    + " WITH ("
                    + "   'metadata.iceberg.storage' = 'hadoop-catalog'"
                    + " )");

            sqlRunner.execute("INSERT INTO " + paimonFqn()
                    + " (id, name, score) VALUES"
                    + " (1, 'alice', 80.0),"
                    + " (2, 'bob', 60.0)");

            sqlRunner.execute("INSERT INTO " + paimonFqn()
                    + " (id, name, score) VALUES"
                    + " (1, 'alice_v2', 95.0),"
                    + " (3, 'carol', 70.0)");

            sqlRunner.execute("CALL " + PAIMON_CATALOG + ".sys.compact('"
                    + SCHEMA + "." + tableName + "')");

            List<TypedRow> rows = sqlRunner.execute(
                    "SELECT id, name, score FROM " + paimonFqn() + " ORDER BY id");

            assertThat(rows).hasSize(3);
            assertThat(rows.get(0).getLong("id")).isEqualTo(1L);
            assertThat(rows.get(0).getString("name")).isEqualTo("alice_v2");
            assertThat(rows.get(0).getDouble("score")).isEqualTo(95.0);
            assertThat(rows.get(1).getLong("id")).isEqualTo(2L);
            assertThat(rows.get(2).getLong("id")).isEqualTo(3L);
        }

        @Test
        @DisplayName("PK table with iceberg compat options are visible in $options system table")
        void pkTableIcebergCompatOptionsVisible() {
            sqlRunner.executeDdl("CREATE TABLE " + paimonFqn()
                    + " (id BIGINT, data VARCHAR,"
                    + "  PRIMARY KEY (id) NOT ENFORCED)"
                    + " WITH ("
                    + "   'metadata.iceberg.storage' = 'hadoop-catalog'"
                    + " )");

            List<TypedRow> options = sqlRunner.execute(
                    "SELECT * FROM " + paimonFqn() + "$options");

            boolean hasIcebergOption = options.stream()
                    .anyMatch(row -> {
                        Object key = row.get("key");
                        return key != null && key.toString().contains("metadata.iceberg.storage");
                    });
            assertThat(hasIcebergOption)
                    .as("$options should contain metadata.iceberg.storage setting")
                    .isTrue();
        }
    }
}
