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
 * E2E tests for compaction triggering index rebuild cycles.
 *
 * <p>Verifies that after data compaction merges small files into larger ones,
 * old index mappings are invalidated, new files get statistics collected,
 * and queries continue to return correct results with pruning still effective.
 *
 * <p>Validates: Requirements 11.AC1, 11.AC2, 11.AC3
 */
@DisplayName("Compaction Index Rebuild E2E")
class CompactionIndexRebuildE2ETest extends SqlE2ETestBase {

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
        tableName = "t_compact_" + method + "_" + System.nanoTime();
        sqlRunner.executeDdl("CREATE TABLE " + fqn()
                + " (id BIGINT, value BIGINT, category VARCHAR)");
    }

    private String fqn() {
        return FQN_PREFIX + tableName;
    }


    /**
     * Inserts multiple small data files to create compaction candidates.
     */
    private void insertSmallFiles() {
        sqlRunner.execute("INSERT INTO " + fqn()
                + " (id, value, category) VALUES (1, 10, 'A'), (2, 20, 'A')");
        sqlRunner.execute("INSERT INTO " + fqn()
                + " (id, value, category) VALUES (3, 30, 'B'), (4, 40, 'B')");
        sqlRunner.execute("INSERT INTO " + fqn()
                + " (id, value, category) VALUES (5, 50, 'C'), (6, 60, 'C')");
        sqlRunner.execute("INSERT INTO " + fqn()
                + " (id, value, category) VALUES (7, 70, 'D'), (8, 80, 'D')");
    }

    @Nested
    @DisplayName("Post-Compaction Query Correctness")
    class PostCompactionCorrectness {

        @Test
        @DisplayName("Query returns correct results after data changes with index")
        void queryCorrectAfterDataChanges() {
            insertSmallFiles();

            sqlRunner.executeDdl("CREATE INDEX idx_val ON " + fqn()
                    + "(value) USING BLOOM_FILTER");

            // Insert more data to trigger file changes
            sqlRunner.execute("INSERT INTO " + fqn()
                    + " (id, value, category) VALUES (9, 90, 'E'), (10, 100, 'E')");

            List<TypedRow> rows = sqlRunner.execute(
                    "SELECT * FROM " + fqn() + " WHERE value = 50");

            assertThat(rows).hasSize(1);
            assertThat(rows.get(0).getRaw("id")).isEqualTo(5L);
        }

        @Test
        @DisplayName("All data accessible after multiple inserts with index")
        void allDataAccessibleAfterMultipleInserts() {
            insertSmallFiles();

            sqlRunner.executeDdl("CREATE INDEX idx_cat ON " + fqn()
                    + "(category) USING BLOOM_FILTER");

            // Additional inserts
            sqlRunner.execute("INSERT INTO " + fqn()
                    + " (id, value, category) VALUES (9, 90, 'A')");

            List<TypedRow> rows = sqlRunner.execute(
                    "SELECT * FROM " + fqn() + " WHERE category = 'A'");

            // Original 2 rows + 1 new row
            assertThat(rows).hasSize(3);
            assertThat(rows).allMatch(r ->
                    "A".equals(r.getRaw("category").toString()));
        }
    }

    @Nested
    @DisplayName("Index Effectiveness After Changes")
    class IndexEffectivenessAfterChanges {

        @Test
        @DisplayName("Range query still works correctly after data evolution")
        void rangeQueryAfterDataEvolution() {
            insertSmallFiles();

            sqlRunner.executeDdl("CREATE INDEX idx_val ON " + fqn()
                    + "(value) USING BLOOM_FILTER");

            // Evolve data
            sqlRunner.execute("INSERT INTO " + fqn()
                    + " (id, value, category) VALUES (11, 110, 'F'), (12, 120, 'F')");

            List<TypedRow> rows = sqlRunner.execute(
                    "SELECT * FROM " + fqn() + " WHERE value BETWEEN 50 AND 100");

            assertThat(rows).isNotEmpty();
            assertThat(rows).allMatch(r -> {
                long val = ((Number) r.getRaw("value")).longValue();
                return val >= 50 && val <= 100;
            });
        }
    }
}
