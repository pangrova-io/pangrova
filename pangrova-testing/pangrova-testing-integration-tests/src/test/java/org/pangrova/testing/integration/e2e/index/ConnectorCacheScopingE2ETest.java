package org.pangrova.testing.integration.e2e.index;

import org.pangrova.spi.connector.data.value.TypedRow;
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
 * E2E tests for connector boundsCache per-query scoping.
 *
 * <p>Verifies that after inserting new data between queries, the second query
 * does not use stale cached bounds from the first query. The boundsCache
 * must be cleared between queries to ensure correctness.
 *
 * <p>Validates: Requirements 8.AC1, 8.AC2
 */
@DisplayName("Connector Cache Scoping E2E")
class ConnectorCacheScopingE2ETest extends SqlE2ETestBase {

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
        tableName = "t_cache_" + method + "_" + System.nanoTime();
        sqlRunner.executeDdl("CREATE TABLE " + fqn()
                + " (id BIGINT, value BIGINT, tag VARCHAR)");
    }

    private String fqn() {
        return FQN_PREFIX + tableName;
    }


    @Nested
    @DisplayName("Cache Scoping Between Queries")
    class CacheScopingBetweenQueries {

        @Test
        @DisplayName("Second query sees new data inserted after first query")
        void secondQuerySeesNewData() {
            // Insert initial data
            sqlRunner.execute("INSERT INTO " + fqn()
                    + " (id, value, tag) VALUES (1, 100, 'batch1'), (2, 200, 'batch1')");

            sqlRunner.executeDdl("CREATE INDEX idx_tag ON " + fqn()
                    + "(tag) USING BLOOM_FILTER");

            // Q1: query initial data
            List<TypedRow> q1 = sqlRunner.execute(
                    "SELECT * FROM " + fqn() + " WHERE tag = 'batch1'");
            assertThat(q1).hasSize(2);

            // Insert more data between queries
            sqlRunner.execute("INSERT INTO " + fqn()
                    + " (id, value, tag) VALUES (3, 300, 'batch2'), (4, 400, 'batch2')");

            // Q2: should see new data — cache must not serve stale bounds
            List<TypedRow> q2 = sqlRunner.execute(
                    "SELECT * FROM " + fqn() + " WHERE tag = 'batch2'");
            assertThat(q2).hasSize(2);
        }

        @Test
        @DisplayName("Total row count reflects all inserts across queries")
        void totalRowCountReflectsAllInserts() {
            sqlRunner.execute("INSERT INTO " + fqn()
                    + " (id, value, tag) VALUES (1, 10, 'a'), (2, 20, 'a')");

            sqlRunner.executeDdl("CREATE INDEX idx_val ON " + fqn()
                    + "(value) USING BLOOM_FILTER");

            // Q1
            List<TypedRow> q1 = sqlRunner.execute("SELECT * FROM " + fqn());
            assertThat(q1).hasSize(2);

            // Insert more
            sqlRunner.execute("INSERT INTO " + fqn()
                    + " (id, value, tag) VALUES (3, 30, 'b'), (4, 40, 'b')");

            // Q2 must include new data
            List<TypedRow> q2 = sqlRunner.execute("SELECT * FROM " + fqn());
            assertThat(q2).hasSize(4);
        }
    }

    @Nested
    @DisplayName("Index Query After Data Evolution")
    class IndexQueryAfterDataEvolution {

        @Test
        @DisplayName("Equality query returns correct results after data evolution")
        void equalityQueryAfterEvolution() {
            sqlRunner.execute("INSERT INTO " + fqn()
                    + " (id, value, tag) VALUES (1, 100, 'x')");

            sqlRunner.executeDdl("CREATE INDEX idx_val ON " + fqn()
                    + "(value) USING BLOOM_FILTER");

            // Query before evolution
            List<TypedRow> before = sqlRunner.execute(
                    "SELECT * FROM " + fqn() + " WHERE value = 100");
            assertThat(before).hasSize(1);

            // Evolve data
            sqlRunner.execute("INSERT INTO " + fqn()
                    + " (id, value, tag) VALUES (2, 100, 'y')");

            // Query after evolution — should see both rows
            List<TypedRow> after = sqlRunner.execute(
                    "SELECT * FROM " + fqn() + " WHERE value = 100");
            assertThat(after).hasSize(2);
        }
    }
}
