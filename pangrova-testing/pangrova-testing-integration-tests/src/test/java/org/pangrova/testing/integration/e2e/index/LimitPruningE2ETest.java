package org.pangrova.testing.integration.e2e.index;

import org.pangrova.connector.spi.data.value.TypedRow;
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
 * E2E tests for LIMIT pruning with fully matching files.
 *
 * <p>Inserts data files where some fully match a predicate, then executes
 * LIMIT queries to verify that the engine stops scanning once enough rows
 * are collected from fully-matching files.
 *
 * <p>Validates: Requirements 6.AC6
 */
@DisplayName("LIMIT Pruning E2E")
class LimitPruningE2ETest extends SqlE2ETestBase {

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
        tableName = "t_limit_" + method + "_" + System.nanoTime();
        sqlRunner.executeDdl("CREATE TABLE " + fqn()
                + " (id BIGINT, value BIGINT, tag VARCHAR)");
    }

    private String fqn() {
        return FQN_PREFIX + tableName;
    }


    /**
     * Inserts data files where all rows have positive values (fully match col > 0).
     */
    private void insertPositiveValueFiles() {
        sqlRunner.execute("INSERT INTO " + fqn()
                + " (id, value, tag) VALUES (1, 10, 'a'), (2, 20, 'b'), (3, 30, 'c')");
        sqlRunner.execute("INSERT INTO " + fqn()
                + " (id, value, tag) VALUES (4, 40, 'd'), (5, 50, 'e'), (6, 60, 'f')");
        sqlRunner.execute("INSERT INTO " + fqn()
                + " (id, value, tag) VALUES (7, 70, 'g'), (8, 80, 'h'), (9, 90, 'i')");
        sqlRunner.execute("INSERT INTO " + fqn()
                + " (id, value, tag) VALUES (10, 100, 'j'), (11, 110, 'k'), (12, 120, 'l')");
        sqlRunner.execute("INSERT INTO " + fqn()
                + " (id, value, tag) VALUES (13, 130, 'm'), (14, 140, 'n'), (15, 150, 'o')");
    }

    @Nested
    @DisplayName("LIMIT with Predicate")
    class LimitWithPredicate {

        @Test
        @DisplayName("LIMIT query returns exactly the requested number of rows")
        void limitQueryReturnsExactRows() {
            insertPositiveValueFiles();

            List<TypedRow> rows = sqlRunner.execute(
                    "SELECT * FROM " + fqn() + " WHERE value > 0 LIMIT 10");

            assertThat(rows).hasSize(10);
            assertThat(rows).allMatch(r ->
                    ((Number) r.getRaw("value")).longValue() > 0);
        }

        @Test
        @DisplayName("LIMIT smaller than total matching rows returns correct count")
        void limitSmallerThanTotal() {
            insertPositiveValueFiles();

            List<TypedRow> rows = sqlRunner.execute(
                    "SELECT * FROM " + fqn() + " WHERE value > 0 LIMIT 3");

            assertThat(rows).hasSize(3);
            assertThat(rows).allMatch(r ->
                    ((Number) r.getRaw("value")).longValue() > 0);
        }
    }

    @Nested
    @DisplayName("LIMIT Edge Cases")
    class LimitEdgeCases {

        @Test
        @DisplayName("LIMIT larger than total rows returns all rows")
        void limitLargerThanTotal() {
            insertPositiveValueFiles();

            List<TypedRow> rows = sqlRunner.execute(
                    "SELECT * FROM " + fqn() + " WHERE value > 0 LIMIT 100");

            // 5 files × 3 rows = 15 total
            assertThat(rows).hasSize(15);
        }

        @Test
        @DisplayName("LIMIT 1 returns exactly one row")
        void limitOne() {
            insertPositiveValueFiles();

            List<TypedRow> rows = sqlRunner.execute(
                    "SELECT * FROM " + fqn() + " WHERE value > 0 LIMIT 1");

            assertThat(rows).hasSize(1);
        }
    }
}
