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
 * E2E tests for idempotent commit log processing.
 *
 * <p>Verifies that inserting data multiple times and querying returns correct
 * results without duplicates. The commit log processing ensures that
 * re-processing events is a no-op (no duplicate builds or statistics).
 *
 * <p>Validates: Requirements 4.AC4
 */
@DisplayName("Idempotent Commit Processing E2E")
class IdempotentCommitProcessingE2ETest extends SqlE2ETestBase {

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
        tableName = "t_idempotent_" + method + "_" + System.nanoTime();
        sqlRunner.executeDdl("CREATE TABLE " + fqn()
                + " (id BIGINT, value BIGINT, label VARCHAR)");
    }

    private String fqn() {
        return FQN_PREFIX + tableName;
    }


    @Nested
    @DisplayName("Multiple Inserts Without Duplicates")
    class MultipleInsertsWithoutDuplicates {

        @Test
        @DisplayName("Multiple INSERT batches produce no duplicate rows")
        void multipleInsertBatchesNoDuplicates() {
            sqlRunner.execute("INSERT INTO " + fqn()
                    + " (id, value, label) VALUES (1, 100, 'batch_1')");
            sqlRunner.execute("INSERT INTO " + fqn()
                    + " (id, value, label) VALUES (2, 200, 'batch_2')");
            sqlRunner.execute("INSERT INTO " + fqn()
                    + " (id, value, label) VALUES (3, 300, 'batch_3')");
            sqlRunner.execute("INSERT INTO " + fqn()
                    + " (id, value, label) VALUES (4, 400, 'batch_4')");
            sqlRunner.execute("INSERT INTO " + fqn()
                    + " (id, value, label) VALUES (5, 500, 'batch_5')");

            // Each INSERT is a separate commit — verify no duplicates
            List<TypedRow> rows = sqlRunner.execute("SELECT * FROM " + fqn());
            assertThat(rows).hasSize(5);

            // Verify each id appears exactly once
            List<Long> ids = rows.stream()
                    .map(r -> ((Number) r.getRaw("id")).longValue())
                    .sorted()
                    .toList();
            assertThat(ids).containsExactly(1L, 2L, 3L, 4L, 5L);
        }

        @Test
        @DisplayName("Query by specific id returns exactly one row")
        void queryByIdReturnsExactlyOneRow() {
            sqlRunner.execute("INSERT INTO " + fqn()
                    + " (id, value, label) VALUES (1, 100, 'a')");
            sqlRunner.execute("INSERT INTO " + fqn()
                    + " (id, value, label) VALUES (2, 200, 'b')");
            sqlRunner.execute("INSERT INTO " + fqn()
                    + " (id, value, label) VALUES (3, 300, 'c')");

            for (long targetId = 1; targetId <= 3; targetId++) {
                List<TypedRow> rows = sqlRunner.execute(
                        "SELECT * FROM " + fqn() + " WHERE id = " + targetId);
                assertThat(rows).hasSize(1);
            }
        }
    }

    @Nested
    @DisplayName("Idempotent Processing with Index")
    class IdempotentProcessingWithIndex {

        @Test
        @DisplayName("Index query after multiple commits returns correct results")
        void indexQueryAfterMultipleCommits() {
            sqlRunner.execute("INSERT INTO " + fqn()
                    + " (id, value, label) VALUES (1, 100, 'x')");
            sqlRunner.execute("INSERT INTO " + fqn()
                    + " (id, value, label) VALUES (2, 200, 'y')");
            sqlRunner.execute("INSERT INTO " + fqn()
                    + " (id, value, label) VALUES (3, 100, 'z')");

            sqlRunner.executeDdl("CREATE INDEX idx_val ON " + fqn()
                    + "(value) USING BLOOM_FILTER");

            // Query with index — should find exactly 2 rows with value=100
            List<TypedRow> rows = sqlRunner.execute(
                    "SELECT * FROM " + fqn() + " WHERE value = 100");

            assertThat(rows).hasSize(2);
            assertThat(rows).allMatch(r ->
                    ((Number) r.getRaw("value")).longValue() == 100L);
        }
    }
}
