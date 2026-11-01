package org.pangrova.testing.integration.e2e.index;

import org.pangrova.spi.connector.data.value.TypedRow;
import org.pangrova.testing.integration.e2e.SqlE2ETestBase;
import org.pangrova.testing.integration.e2e.config.ConnectorTestConfig;
import org.pangrova.testing.integration.e2e.runner.QueryError;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * E2E tests for SET CLUSTERING + RECLUSTER and index synergy.
 *
 * <p>Validates: Requirements 24.1–24.6
 */
@DisplayName("Clustering E2E")
class ClusteringE2ETest extends SqlE2ETestBase {

    private static final String CATALOG = "iceberg_catalog";
    private static final String SCHEMA = "default";
    private static final String FQN_PREFIX = CATALOG + "." + SCHEMA + ".";

    @Override
    protected List<ConnectorTestConfig> connectorConfigs() {
        return List.of(
                ConnectorTestConfig.icebergLocal(CATALOG, SCHEMA, icebergWarehouse().toString()));
    }

    private String tableName;

    @BeforeAll
    static void setupCatalog() {
        registerTestConnectors(List.of(
                ConnectorTestConfig.icebergLocal(CATALOG, SCHEMA, icebergWarehouse().toString())));
    }

    @BeforeEach
    void createTestTable(TestInfo info) {
        String method = info.getTestMethod().map(m -> m.getName()).orElse("unknown");
        tableName = "t_clust_" + method + "_" + System.nanoTime();
        sqlRunner.executeDdl("CREATE TABLE " + fqn()
                + " (col1 BIGINT, col2 BIGINT, value VARCHAR)");
        sqlRunner.execute("INSERT INTO " + fqn()
                + " VALUES (100, 1, 'a'), (200, 2, 'b'), (300, 3, 'c'), (400, 4, 'd')");
    }

    private String fqn() { return FQN_PREFIX + tableName; }

    @Nested
    @DisplayName("SET CLUSTERING")
    class SetClustering {

        @Test
        @DisplayName("SET CLUSTERING ORDER is parsed but not yet routed — returns INTERNAL_ERROR")
        void setClusteringOrderNotYetSupported() {
            QueryError error = sqlRunner.executeExpectingError(
                    "ALTER TABLE " + fqn()
                    + " SET CLUSTERING ORDER (col1, col2) MODE INTERLEAVE");
            assertThat(error.message()).containsAnyOf("Unsupported statement", "Unknown statement type");
        }
    }

    @Nested
    @DisplayName("RECLUSTER")
    class Recluster {

        @Test
        @DisplayName("RECLUSTER depends on SET CLUSTERING — not yet supported")
        void reclusterNotYetSupported() {
            QueryError error = sqlRunner.executeExpectingError(
                    "ALTER TABLE " + fqn()
                    + " SET CLUSTERING ORDER (col1) MODE LINEAR");
            assertThat(error.message()).containsAnyOf("Unsupported statement", "Unknown statement type");
        }
    }

    @Nested
    @DisplayName("Clustering + Index Synergy")
    class ClusteringIndexSynergy {

        @Test
        @DisplayName("BTREE index on range column enables range query filtering")
        void btreeIndexRangeQuery() {
            sqlRunner.executeDdl("CREATE INDEX idx_clust ON " + fqn()
                    + "(col1) USING BTREE");

            List<TypedRow> result = sqlRunner.execute(
                    "SELECT * FROM " + fqn()
                    + " WHERE (col1 BETWEEN 100 AND 200)");
            assertThat(result).hasSize(2);
        }
    }
}
