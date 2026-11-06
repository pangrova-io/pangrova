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
 * E2E tests for availability window correctness during UPDATE (COW) operations.
 *
 * <p>Verifies that after an UPDATE replaces data files, new (unindexed) files
 * are not incorrectly pruned by L1 index evaluation. The availability window
 * ensures files newer than the last indexed snapshot bypass L1 pruning.
 *
 * <p>Validates: Requirements 3.AC1, 3.AC2, 3.AC4
 */
@DisplayName("Availability Window E2E")
class AvailabilityWindowE2ETest extends SqlE2ETestBase {

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
        tableName = "t_avail_" + method + "_" + System.nanoTime();
        sqlRunner.executeDdl("CREATE TABLE " + fqn()
                + " (id BIGINT, status VARCHAR, amount DOUBLE)");
    }

    private String fqn() {
        return FQN_PREFIX + tableName;
    }


    @Nested
    @DisplayName("UPDATE Correctness")
    class UpdateCorrectness {

        @Test
        @DisplayName("Query returns updated rows after UPDATE with index present")
        void queryReturnsUpdatedRowsWithIndex() {
            // Insert initial data across multiple files
            sqlRunner.execute("INSERT INTO " + fqn()
                    + " (id, status, amount) VALUES (1, 'ACTIVE', 100.0), (2, 'ACTIVE', 200.0)");
            sqlRunner.execute("INSERT INTO " + fqn()
                    + " (id, status, amount) VALUES (3, 'PENDING', 300.0), (4, 'PENDING', 400.0)");

            // Create index on existing data
            sqlRunner.executeDdl("CREATE INDEX idx_status ON " + fqn()
                    + "(status) USING BLOOM_FILTER");

            // UPDATE replaces some files (COW creates new files)
            sqlRunner.execute("UPDATE " + fqn()
                    + " SET status = 'COMPLETED' WHERE id = 1");

            // Query should see the updated row — new files must not be pruned by L1
            List<TypedRow> rows = sqlRunner.execute(
                    "SELECT * FROM " + fqn() + " WHERE status = 'COMPLETED'");

            assertThat(rows).hasSize(1);
            assertThat(rows.get(0).getRaw("id")).isEqualTo(1L);
        }

        @Test
        @DisplayName("Query returns both old and new data after partial UPDATE")
        void queryReturnsBothOldAndNewData() {
            sqlRunner.execute("INSERT INTO " + fqn()
                    + " (id, status, amount) VALUES (1, 'ACTIVE', 100.0), (2, 'ACTIVE', 200.0)");
            sqlRunner.execute("INSERT INTO " + fqn()
                    + " (id, status, amount) VALUES (3, 'ACTIVE', 300.0), (4, 'ACTIVE', 400.0)");

            sqlRunner.executeDdl("CREATE INDEX idx_status ON " + fqn()
                    + "(status) USING BLOOM_FILTER");

            // Update only one row — creates new file for that partition
            sqlRunner.execute("UPDATE " + fqn()
                    + " SET amount = 999.0 WHERE id = 3");

            // Query touching both indexed (old) and unindexed (new) files
            List<TypedRow> rows = sqlRunner.execute(
                    "SELECT * FROM " + fqn() + " WHERE status = 'ACTIVE'");

            // All 4 rows should still have status ACTIVE (UPDATE only changed amount)
            assertThat(rows).hasSize(4);
        }
    }

    @Nested
    @DisplayName("Availability Window Bypass")
    class AvailabilityWindowBypass {

        @Test
        @DisplayName("New files after index creation are not pruned by L1")
        void newFilesNotPrunedByL1() {
            sqlRunner.execute("INSERT INTO " + fqn()
                    + " (id, status, amount) VALUES (1, 'ACTIVE', 100.0)");

            sqlRunner.executeDdl("CREATE INDEX idx_status ON " + fqn()
                    + "(status) USING BLOOM_FILTER");

            // Insert new data after index creation — these files are outside the availability window
            sqlRunner.execute("INSERT INTO " + fqn()
                    + " (id, status, amount) VALUES (2, 'ACTIVE', 200.0)");

            List<TypedRow> rows = sqlRunner.execute(
                    "SELECT * FROM " + fqn() + " WHERE status = 'ACTIVE'");

            // Both old (indexed) and new (unindexed) rows must be returned
            assertThat(rows).hasSize(2);
        }
    }
}
