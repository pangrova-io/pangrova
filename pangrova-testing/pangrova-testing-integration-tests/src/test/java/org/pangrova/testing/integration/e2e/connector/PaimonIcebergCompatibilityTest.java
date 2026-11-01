package org.pangrova.testing.integration.e2e.connector;

import org.pangrova.spi.connector.data.value.TypedRow;
import org.pangrova.testing.integration.e2e.SqlE2ETestBase;
import org.pangrova.testing.integration.e2e.config.ConnectorTestConfig;
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
 * persist the compatibility setting and remain readable through the Paimon connector.
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
                        ICEBERG_CATALOG, SCHEMA, paimonWarehouse().resolve("iceberg").toString()));
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

    private String icebergFqn() {
        return ICEBERG_FQN_PREFIX + tableName;
    }

    private String persistedIcebergStorage() {
        return sqlRunner.execute("SELECT * FROM " + paimonFqn() + "$options").stream()
                .filter(row -> "metadata.iceberg.storage".equals(row.getString("key")))
                .map(row -> row.getString("value"))
                .findFirst().orElse(null);
    }

    // --- 2.14.1: Paimon read after Iceberg compatibility option is set ---

    @Nested
    @DisplayName("2.14.1 - Iceberg Compatibility Append Table")
    class IcebergCompatAppendTable {

        @Test
        @DisplayName("Paimon append table is readable through its published Iceberg catalog")
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

            List<TypedRow> icebergRows = sqlRunner.execute(
                    "SELECT id, name, amount FROM " + icebergFqn() + " ORDER BY id");
            assertThat(icebergRows).hasSize(3);
            assertThat(icebergRows.get(0).getString("name")).isEqualTo("alice");
            assertThat(icebergRows.get(1).getString("name")).isEqualTo("bob");
            assertThat(icebergRows.get(2).getString("name")).isEqualTo("carol");

            sqlRunner.execute("INSERT INTO " + paimonFqn()
                    + " (id, name, amount) VALUES (4, 'dana', 400.0)");
            List<TypedRow> refreshed = sqlRunner.execute(
                    "SELECT id, name FROM " + icebergFqn() + " ORDER BY id");
            assertThat(refreshed).hasSize(4);
            assertThat(refreshed.get(3).getString("name")).isEqualTo("dana");
        }

        @Test
        @DisplayName("Paimon append table remains readable with Iceberg compatibility enabled")
        void appendTableRemainsReadableWithCompatibilityEnabled() {
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

    // --- 2.14.2: PK table stays readable after compaction ---

    @Nested
    @DisplayName("2.14.2 - PK Table Iceberg Compat Compaction")
    class PkTableIcebergCompatCompaction {

        @Test
        @DisplayName("PK table with Iceberg compatibility remains readable after compaction")
        void pkTableRemainsReadableAfterCompaction() {
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

            List<TypedRow> icebergRows = sqlRunner.execute(
                    "SELECT id, name, score FROM " + icebergFqn() + " ORDER BY id");
            assertThat(icebergRows).hasSize(3);
            assertThat(icebergRows.get(0).getString("name")).isEqualTo("alice_v2");
            assertThat(icebergRows.get(0).getDouble("score")).isEqualTo(95.0);
        }

        @Test
        @DisplayName("PK table persists Iceberg compatibility option")
        void pkTableIcebergCompatOptionsVisible() throws Exception {
            sqlRunner.executeDdl("CREATE TABLE " + paimonFqn()
                    + " (id BIGINT, data VARCHAR,"
                    + "  PRIMARY KEY (id) NOT ENFORCED)"
                    + " WITH ("
                    + "   'metadata.iceberg.storage' = 'hadoop-catalog'"
                    + " )");

            assertThat(persistedIcebergStorage()).isEqualTo("hadoop-catalog");
        }
    }
}
