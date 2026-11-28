package org.pangrova.testing.integration.e2e.index;

import org.pangrova.types.value.TypedRow;
import org.pangrova.testing.integration.e2e.SqlE2ETestBase;
import org.pangrova.testing.integration.e2e.config.ConnectorTestConfig;
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
        @DisplayName("Iceberg publishes interleaved clustering")
        void interleavedRewritePreservesRows() {
            sqlRunner.executeDdl("ALTER TABLE " + fqn() + " SET CLUSTERING ORDER (col1, col2) MODE INTERLEAVE");
            sqlRunner.execute("RECLUSTER TABLE " + fqn());
            assertThat(sqlRunner.execute("SHOW CLUSTERING STATUS FOR TABLE " + fqn()))
                    .singleElement().satisfies(status -> {
                        assertThat(status.getString("mode")).isEqualTo("INTERLEAVE");
                        assertThat(status.getString("state")).isEqualTo("CLUSTERED");
                    });
            assertThat(sqlRunner.execute("SELECT * FROM " + fqn())).hasSize(4);
        }
    }

    @Nested
    @DisplayName("RECLUSTER")
    class Recluster {

        @Test
        @DisplayName("LINEAR rewrite records a successful run and preserves all rows")
        void linearRewritePreservesRows() {
            sqlRunner.executeDdl("ALTER TABLE " + fqn() + " SET CLUSTERING ORDER (col1) MODE LINEAR");
            sqlRunner.execute("RECLUSTER TABLE " + fqn());

            var rows = sqlRunner.execute("SHOW CLUSTERING STATUS FOR TABLE " + fqn());
            assertThat(rows).singleElement().satisfies(status -> {
                assertThat(status.getString("state")).isEqualTo("CLUSTERED");
                assertThat(status.getString("task_state")).isEqualTo("SUCCEEDED");
            });
            assertThat(sqlRunner.execute("SELECT * FROM " + fqn())).hasSize(4);
        }
    }

    @Nested
    @DisplayName("Clustering + Index Synergy")
    class ClusteringIndexSynergy {

        @Test
        @DisplayName("BTREE index on range column enables range query filtering")
        void btreeIndexRangeQuery() {
            sqlRunner.executeDdl("CREATE INDEX idx_clust ON " + fqn()
                    + "(col1) USING BTREE FORCE");

            List<TypedRow> result = sqlRunner.execute(
                    "SELECT * FROM " + fqn()
                    + " WHERE (col1 BETWEEN 100 AND 200)");
            assertThat(result).hasSize(2);
        }
    }
}
