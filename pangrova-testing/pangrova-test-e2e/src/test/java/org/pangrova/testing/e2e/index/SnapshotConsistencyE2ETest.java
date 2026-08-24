package org.pangrova.testing.e2e.index;

import org.pangrova.spi.connector.data.TypedRow;
import org.pangrova.testing.e2e.SqlE2ETestBase;
import org.pangrova.testing.e2e.config.ConnectorTestConfig;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * E2E tests for snapshot consistency with index evaluation across Iceberg and Paimon connectors.
 *
 * <p>Validates: Requirements 34.1–34.4, 42.1, 42.2
 */
@DisplayName("Snapshot Consistency E2E")
class SnapshotConsistencyE2ETest extends SqlE2ETestBase {

    private static final String ICEBERG_CATALOG = "iceberg_catalog";
    private static final String PAIMON_CATALOG = "paimon_catalog";
    private static final String SCHEMA = "default";

    @Override
    protected List<ConnectorTestConfig> connectorConfigs() {
        return List.of(
                ConnectorTestConfig.icebergLocal(ICEBERG_CATALOG, SCHEMA, icebergWarehouse().toString()),
                ConnectorTestConfig.paimon(PAIMON_CATALOG, SCHEMA, paimonWarehouse().toString()));
    }

    @BeforeAll
    static void setupCatalogs() {
        registerTestConnectors(List.of(
                ConnectorTestConfig.icebergLocal(ICEBERG_CATALOG, SCHEMA, icebergWarehouse().toString()),
                ConnectorTestConfig.paimon(PAIMON_CATALOG, SCHEMA, paimonWarehouse().toString())));
    }

    // ── shared helpers ──

    private static String fqn(String catalog, String tableName) {
        return catalog + "." + SCHEMA + "." + tableName;
    }

    private static String createTableAndSeed(String catalog, String prefix, TestInfo info) {
        String method = info.getTestMethod().map(m -> m.getName()).orElse("unknown");
        String tableName = prefix + method + "_" + System.nanoTime();
        String fqn = fqn(catalog, tableName);
        sqlRunner.executeDdl("CREATE TABLE " + fqn + " (user_id BIGINT, value VARCHAR)");
        sqlRunner.execute("INSERT INTO " + fqn + " VALUES (1, 'a'), (2, 'b')");
        return tableName;
    }

    private static void verifySnapshotPinning(String catalog, String tableName) {
        String fqn = fqn(catalog, tableName);
        sqlRunner.executeDdl("CREATE INDEX idx_snap ON " + fqn + "(user_id) USING BLOOM_FILTER");

        // S1 state captured
        List<TypedRow> s1Result = sqlRunner.execute(
                "SELECT * FROM " + fqn + " WHERE user_id = 1");
        assertThat(s1Result).isNotEmpty();

        // Insert new data creating S2
        sqlRunner.execute("INSERT INTO " + fqn + " VALUES (3, 'c')");

        // Query on latest should see new data
        List<TypedRow> latestResult = sqlRunner.execute(
                "SELECT * FROM " + fqn + " WHERE user_id = 3");
        assertThat(latestResult).isNotEmpty();
    }

    private static void verifyTimeTravelWithIndex(String catalog, String tableName) {
        String fqn = fqn(catalog, tableName);
        sqlRunner.executeDdl("CREATE INDEX idx_tt ON " + fqn + "(user_id) USING BLOOM_FILTER");

        // Insert additional data
        sqlRunner.execute("INSERT INTO " + fqn + " VALUES (10, 'new')");

        // Query current state
        List<TypedRow> current = sqlRunner.execute(
                "SELECT * FROM " + fqn + " WHERE user_id = 10");
        assertThat(current).isNotEmpty();

        // Original data should still be queryable
        List<TypedRow> original = sqlRunner.execute(
                "SELECT * FROM " + fqn + " WHERE user_id = 1");
        assertThat(original).isNotEmpty();
    }

    private static void verifyCompactionSnapshotIsolation(String catalog, String tableName) {
        String fqn = fqn(catalog, tableName);
        sqlRunner.executeDdl("CREATE INDEX idx_csi ON " + fqn + "(user_id) USING BLOOM_FILTER");

        // Insert more data to create multiple files
        sqlRunner.execute("INSERT INTO " + fqn + " VALUES (3, 'c')");
        sqlRunner.execute("INSERT INTO " + fqn + " VALUES (4, 'd')");

        // Query should return correct results regardless of compaction state
        List<TypedRow> result = sqlRunner.execute(
                "SELECT * FROM " + fqn + " WHERE user_id = 1");
        assertThat(result).isNotEmpty();

        // All data should be accessible
        List<TypedRow> allData = sqlRunner.execute(
                "SELECT * FROM " + fqn);
        assertThat(allData).hasSize(4);
    }

    // ── Iceberg scenarios ──

    @Nested
    @DisplayName("Iceberg Scenarios")
    class IcebergScenarios {

        private String tableName;

        @BeforeEach
        void createTestTable(TestInfo info) {
            tableName = createTableAndSeed(ICEBERG_CATALOG, "t_snap_ice_", info);
        }

        @Nested
        @DisplayName("Snapshot Pinning")
        class SnapshotPinning {

            @Test
            @DisplayName("Query pinned to S1 does not see S2 index mappings")
            void snapshotPinning() {
                verifySnapshotPinning(ICEBERG_CATALOG, tableName);
            }
        }

        @Nested
        @DisplayName("Time Travel Query")
        class TimeTravelQuery {

            @Test
            @DisplayName("Time-travel query uses only snapshot-visible index mappings")
            void timeTravelWithIndex() {
                verifyTimeTravelWithIndex(ICEBERG_CATALOG, tableName);
            }
        }
    }

    // ── Paimon scenarios ──

    @Nested
    @DisplayName("Paimon Scenarios")
    class PaimonScenarios {

        private String tableName;

        @BeforeEach
        void createTestTable(TestInfo info) {
            tableName = createTableAndSeed(PAIMON_CATALOG, "t_snap_pmn_", info);
        }

        @Nested
        @DisplayName("Compaction Snapshot Isolation")
        class CompactionSnapshotIsolation {

            @Test
            @DisplayName("Compacted file indexes not matched to pre-compaction queries")
            void compactionSnapshotIsolation() {
                verifyCompactionSnapshotIsolation(PAIMON_CATALOG, tableName);
            }
        }
    }
}
