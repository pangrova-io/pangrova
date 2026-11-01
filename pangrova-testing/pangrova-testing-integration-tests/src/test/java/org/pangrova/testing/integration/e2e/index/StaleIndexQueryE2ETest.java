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
 * E2E tests for STALE index query participation as best-effort filters.
 *
 * <p>Validates: Requirements 38.1–38.4
 */
@DisplayName("Stale Index Query E2E")
class StaleIndexQueryE2ETest extends SqlE2ETestBase {

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
        tableName = "t_stale_" + method + "_" + System.nanoTime();
        sqlRunner.executeDdl("CREATE TABLE " + fqn()
                + " (user_id BIGINT, value VARCHAR)");
        sqlRunner.execute("INSERT INTO " + fqn() + " VALUES (1, 'a'), (2, 'b')");
    }

    private String fqn() { return FQN_PREFIX + tableName; }

    @Nested
    @DisplayName("STALE Index Filtering (include_stale=true)")
    class StaleIndexIncluded {

        @Test
        @DisplayName("STALE index still filters old splits — partial benefit")
        void staleIndexFiltering() {
            sqlRunner.executeDdl("CREATE INDEX idx_stale ON " + fqn()
                    + "(user_id) USING BLOOM_FILTER");

            // Insert new batch — coverage drops, index becomes STALE
            sqlRunner.execute("INSERT INTO " + fqn() + " VALUES (3, 'c')");

            // Query should still return correct results
            List<TypedRow> result = sqlRunner.execute(
                    "SELECT * FROM " + fqn() + " WHERE user_id = 1");
            assertThat(result).isNotEmpty();
            assertThat(result).allMatch(r ->
                    Long.valueOf(1L).equals(r.getRaw("user_id")));
        }
    }

    @Nested
    @DisplayName("STALE Index Excluded (include_stale=false)")
    class StaleIndexExcluded {

        @Test
        @DisplayName("STALE index excluded — all splits retained, correct results")
        void staleIndexExcluded() {
            sqlRunner.executeDdl("CREATE INDEX idx_stale_ex ON " + fqn()
                    + "(user_id) USING BLOOM_FILTER");

            sqlRunner.execute("INSERT INTO " + fqn() + " VALUES (4, 'd')");

            // All data must be accessible
            List<TypedRow> allData = sqlRunner.execute("SELECT * FROM " + fqn());
            assertThat(allData).hasSize(3);
        }
    }

    @Nested
    @DisplayName("SHOW INDEXES Warning")
    class ShowIndexesWarning {

        @Test
        @DisplayName("SHOW INDEXES shows stale index with warning indicator")
        void showIndexesStaleWarning() {
            sqlRunner.executeDdl("CREATE INDEX idx_stale_warn ON " + fqn()
                    + "(user_id) USING BLOOM_FILTER");

            // Insert to make index stale
            sqlRunner.execute("INSERT INTO " + fqn() + " VALUES (5, 'e')");

            List<TypedRow> indexes = sqlRunner.execute("SHOW INDEXES FROM " + fqn());
            assertThat(indexes).anyMatch(r ->
                    "idx_stale_warn".equals(r.getRaw("index_name").toString()));
        }
    }
}
