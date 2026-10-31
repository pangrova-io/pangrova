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
 * E2E tests for L0 + L1 combined pruning using BloomFilter equality queries.
 *
 * <p>Creates data files, builds a BLOOM_FILTER index, then executes equality
 * queries to verify that L0 statistics and L1 BloomFilter evaluation work
 * together to prune files while returning correct results.
 *
 * <p>Validates: Requirements 1.AC1, 2.AC1, 2.AC2
 */
@DisplayName("L0 + L1 Combined Pruning E2E")
class L0L1CombinedPruningE2ETest extends SqlE2ETestBase {

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
        tableName = "t_l0l1_" + method + "_" + System.nanoTime();
        sqlRunner.executeDdl("CREATE TABLE " + fqn()
                + " (id BIGINT, user_id BIGINT, category VARCHAR)");
    }

    private String fqn() {
        return FQN_PREFIX + tableName;
    }


    /**
     * Inserts multiple data files with distinct user_id ranges for combined pruning.
     */
    private void insertDataFiles() {
        sqlRunner.execute("INSERT INTO " + fqn()
                + " (id, user_id, category) VALUES (1, 100, 'A'), (2, 101, 'A')");
        sqlRunner.execute("INSERT INTO " + fqn()
                + " (id, user_id, category) VALUES (3, 200, 'B'), (4, 201, 'B')");
        sqlRunner.execute("INSERT INTO " + fqn()
                + " (id, user_id, category) VALUES (5, 300, 'C'), (6, 301, 'C')");
        sqlRunner.execute("INSERT INTO " + fqn()
                + " (id, user_id, category) VALUES (7, 400, 'D'), (8, 401, 'D')");
        sqlRunner.execute("INSERT INTO " + fqn()
                + " (id, user_id, category) VALUES (9, 500, 'E'), (10, 501, 'E')");
    }

    @Nested
    @DisplayName("BloomFilter Equality Pruning")
    class BloomFilterEquality {

        @Test
        @DisplayName("Equality query with BloomFilter index returns correct results")
        void equalityQueryWithBloomFilter() {
            insertDataFiles();
            sqlRunner.executeDdl("CREATE INDEX idx_bf ON " + fqn()
                    + "(user_id) USING BLOOM_FILTER");

            List<TypedRow> rows = sqlRunner.execute(
                    "SELECT * FROM " + fqn() + " WHERE user_id = 300");

            assertThat(rows).hasSize(1);
            assertThat(rows.get(0).getRaw("id")).isEqualTo(5L);
        }

        @Test
        @DisplayName("Equality query for non-existent value returns empty")
        void equalityQueryNonExistentValue() {
            insertDataFiles();
            sqlRunner.executeDdl("CREATE INDEX idx_bf ON " + fqn()
                    + "(user_id) USING BLOOM_FILTER");

            List<TypedRow> rows = sqlRunner.execute(
                    "SELECT * FROM " + fqn() + " WHERE user_id = 999");

            assertThat(rows).isEmpty();
        }
    }

    @Nested
    @DisplayName("Combined L0 + L1 Filtering")
    class CombinedFiltering {

        @Test
        @DisplayName("Range + equality combined query returns correct results")
        void rangeAndEqualityCombined() {
            insertDataFiles();
            sqlRunner.executeDdl("CREATE INDEX idx_bf ON " + fqn()
                    + "(user_id) USING BLOOM_FILTER");

            List<TypedRow> rows = sqlRunner.execute(
                    "SELECT * FROM " + fqn()
                            + " WHERE id BETWEEN 1 AND 6 AND user_id = 200");

            assertThat(rows).hasSize(1);
            assertThat(rows).allMatch(r -> {
                long id = ((Number) r.getRaw("id")).longValue();
                long uid = ((Number) r.getRaw("user_id")).longValue();
                return id >= 1 && id <= 6 && uid == 200;
            });
        }
    }
}
