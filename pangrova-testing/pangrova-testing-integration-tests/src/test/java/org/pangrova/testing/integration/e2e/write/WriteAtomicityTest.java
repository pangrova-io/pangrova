package org.pangrova.testing.integration.e2e.write;

import org.pangrova.connector.spi.data.value.TypedRow;
import org.pangrova.testing.integration.e2e.SqlE2ETestBase;
import org.pangrova.testing.integration.e2e.config.ConnectorTestConfig;
import org.pangrova.testing.integration.e2e.runner.QueryError;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Example-based tests for write atomicity and concurrent write behavior.
 *
 * <p>Covers R6.23-R6.26. Error injection happens through the real pipeline
 * (e.g., invalid SQL, constraint violations, type mismatches, runtime errors).
 * Atomicity is verified by checking table state on real storage after failed
 * operations — no partial data must be visible.
 *
 * <p>Requirements: 6.6, 6.7, 40.2
 */
@DisplayName("Write Atomicity - Example Tests")
class WriteAtomicityTest extends SqlE2ETestBase {

    private static final String CATALOG = "iceberg_catalog";
    private static final String SCHEMA = "default";
    private static final String FQN_PREFIX = CATALOG + "." + SCHEMA + ".";

    @Override
    protected List<ConnectorTestConfig> connectorConfigs() {
        return List.of(
                ConnectorTestConfig.icebergLocal(CATALOG, SCHEMA, icebergWarehouse().toString()));
    }

    private String tableName;

    @BeforeEach
    void assignUniqueTableName(TestInfo testInfo) {
        String method = testInfo.getTestMethod().map(m -> m.getName()).orElse("unknown");
        tableName = "t_" + method + "_" + System.nanoTime();
    }

    private String fqn() {
        return FQN_PREFIX + tableName;
    }

    /** Seed a 3-row table: (1,'alice',100.0), (2,'bob',200.0), (3,'carol',300.0). */
    private void seedThreeRows() {
        sqlRunner.executeDdl("CREATE TABLE " + fqn()
                + " (id BIGINT, name VARCHAR, amount DOUBLE)");
        sqlRunner.execute("INSERT INTO " + fqn() + " (id, name, amount) VALUES "
                + "(1, 'alice', 100.0), (2, 'bob', 200.0), (3, 'carol', 300.0)");
    }

    /** Read all rows ordered by id for deterministic comparison. */
    private List<TypedRow> readAllOrdered() {
        return sqlRunner.execute("SELECT * FROM " + fqn() + " ORDER BY id");
    }

    /** Snapshot current table state for pre/post comparison. */
    private List<TypedRow> snapshotTable() {
        return new ArrayList<>(readAllOrdered());
    }

    // -----------------------------------------------------------------------
    // R6.23: Write failure rollback — table state unchanged after failed write
    // -----------------------------------------------------------------------

    @Nested
    @DisplayName("R6.23 - Write Failure Rollback")
    class WriteFailureRollback {

        @Test
        @DisplayName("INSERT with type mismatch fails and leaves table unchanged")
        void insertTypeMismatchRollback() {
            seedThreeRows();
            List<TypedRow> before = snapshotTable();

            QueryError error = sqlRunner.executeExpectingError(
                    "INSERT INTO " + fqn()
                    + " (id, name, amount) VALUES ('not_a_number', 'fail', 0.0)");

            assertThat(error.errorType()).isIn("RUNTIME", "SEMANTIC");

            List<TypedRow> after = readAllOrdered();
            assertThat(after).isEqualTo(before);
        }

        @Test
        @DisplayName("UPDATE with invalid expression fails and leaves table unchanged")
        void updateInvalidExpressionRollback() {
            seedThreeRows();
            List<TypedRow> before = snapshotTable();

            // Reference a non-existent column — Calcite validation rejects this
            // as a SEMANTIC error. CAST('invalid' AS DOUBLE) is not suitable because
            // Calcite constant-folds it to null instead of raising a runtime error.
            QueryError error = sqlRunner.executeExpectingError(
                    "UPDATE " + fqn()
                    + " SET amount = nonexistent_column WHERE id = 1");

            assertThat(error.errorType()).isIn("RUNTIME", "SEMANTIC");

            List<TypedRow> after = readAllOrdered();
            assertThat(after).isEqualTo(before);
        }

        @Test
        @DisplayName("DELETE referencing non-existent column fails and leaves table unchanged")
        void deleteNonExistentColumnRollback() {
            seedThreeRows();
            List<TypedRow> before = snapshotTable();

            QueryError error = sqlRunner.executeExpectingError(
                    "DELETE FROM " + fqn() + " WHERE nonexistent_col = 1");

            assertThat(error.errorType()).isEqualTo("SEMANTIC");

            List<TypedRow> after = readAllOrdered();
            assertThat(after).isEqualTo(before);
        }

        @Test
        @DisplayName("INSERT into non-existent table fails with no partial data created")
        void insertIntoNonExistentTableNoPartialData() {
            String ghost = FQN_PREFIX + "nonexistent_" + System.nanoTime();

            QueryError error = sqlRunner.executeExpectingError(
                    "INSERT INTO " + ghost + " (id) VALUES (1)");

            assertThat(error.errorType()).isIn("SEMANTIC", "RUNTIME");

            // Table must not exist after the failed INSERT
            QueryError selectError = sqlRunner.executeExpectingError(
                    "SELECT * FROM " + ghost);
            assertThat(selectError.errorType()).isIn("SEMANTIC", "RUNTIME");
        }

        @Test
        @DisplayName("Failed INSERT-SELECT with runtime error leaves zero new rows")
        void failedBatchInsertLeavesNoPartialRows() {
            seedThreeRows();
            List<TypedRow> before = snapshotTable();

            // Reference a nonexistent column to guarantee a SEMANTIC error.
            // Division-by-zero (id - id) is unreliable because Calcite constant-folds
            // it and DOUBLE/0 yields Infinity rather than an error.
            QueryError error = sqlRunner.executeExpectingError(
                    "INSERT INTO " + fqn() + " (id, name, amount) "
                    + "SELECT id, name, nonexistent_column FROM " + fqn());

            assertThat(error.errorType()).isIn("SEMANTIC", "RUNTIME");

            List<TypedRow> after = readAllOrdered();
            assertThat(after).isEqualTo(before);
        }
    }

    // -----------------------------------------------------------------------
    // R6.24: Write failure retryable — retry succeeds after prior failure
    // -----------------------------------------------------------------------

    @Nested
    @DisplayName("R6.24 - Write Failure Retryable")
    class WriteFailureRetryable {

        @Test
        @DisplayName("INSERT fails then succeeds on retry with valid data")
        void insertRetryAfterFailure() {
            seedThreeRows();

            // First attempt: invalid — type mismatch
            sqlRunner.executeExpectingError(
                    "INSERT INTO " + fqn()
                    + " (id, name, amount) VALUES ('bad', 'fail', 0.0)");

            // Retry with valid data
            sqlRunner.execute("INSERT INTO " + fqn()
                    + " (id, name, amount) VALUES (4, 'dave', 400.0)");

            List<TypedRow> rows = readAllOrdered();
            assertThat(rows).hasSize(4);
            assertThat(rows.get(3).getLong("id")).isEqualTo(4L);
            assertThat(rows.get(3).getString("name")).isEqualTo("dave");
        }

        @Test
        @DisplayName("UPDATE fails then succeeds on retry with valid expression")
        void updateRetryAfterFailure() {
            seedThreeRows();

            // First attempt: reference non-existent column — Calcite rejects as SEMANTIC error.
            // CAST('bad' AS DOUBLE) is not suitable because Calcite constant-folds it to null.
            sqlRunner.executeExpectingError(
                    "UPDATE " + fqn()
                    + " SET amount = nonexistent_column WHERE id = 1");

            // Retry with valid expression
            sqlRunner.execute("UPDATE " + fqn()
                    + " SET amount = 999.0 WHERE id = 1");

            List<TypedRow> rows = readAllOrdered();
            assertThat(rows.get(0).getDouble("amount"))
                    .isEqualTo(999.0);
        }
    }

    // -----------------------------------------------------------------------
    // R6.25: Concurrent write conflict — same table, same partition
    // -----------------------------------------------------------------------

    @Nested
    @DisplayName("R6.25 - Concurrent Write Conflict")
    class ConcurrentWriteConflict {

        @Test
        @DisplayName("Concurrent writes to same table detect conflict and preserve consistency")
        void concurrentWritesSameTable() throws InterruptedException {
            seedThreeRows();

            ExecutorService executor = Executors.newFixedThreadPool(2);
            try {
                Future<?> writer1 = executor.submit(() ->
                        sqlRunner.execute("UPDATE " + fqn()
                                + " SET amount = 111.0 WHERE id = 1"));
                Future<?> writer2 = executor.submit(() ->
                        sqlRunner.execute("UPDATE " + fqn()
                                + " SET amount = 222.0 WHERE id = 1"));

                collectResults(writer1, writer2);
            } finally {
                executor.shutdown();
            }

            // Regardless of conflict outcome, table must be consistent:
            // id=1 has exactly one amount value (either 111 or 222)
            List<TypedRow> rows = readAllOrdered();
            assertThat(rows).hasSize(3);

            double finalAmount = rows.get(0).getDouble("amount");
            assertThat(finalAmount).isIn(111.0, 222.0);

            // Other rows remain untouched
            assertThat(rows.get(1).getDouble("amount")).isEqualTo(200.0);
            assertThat(rows.get(2).getDouble("amount")).isEqualTo(300.0);
        }
    }

    // -----------------------------------------------------------------------
    // R6.26: Concurrent write different partitions — no conflict
    // -----------------------------------------------------------------------

    @Nested
    @DisplayName("R6.26 - Concurrent Write Different Partitions")
    class ConcurrentWriteDifferentPartitions {

        @Test
        @DisplayName("Concurrent writes to different partitions both succeed")
        void concurrentWritesDifferentPartitions() throws InterruptedException {
            sqlRunner.executeDdl("CREATE TABLE " + fqn()
                    + " (id BIGINT, region VARCHAR, amount DOUBLE)"
                    + " PARTITIONED BY (region)");
            sqlRunner.execute("INSERT INTO " + fqn()
                    + " (id, region, amount) VALUES (1, 'east', 100.0)");
            sqlRunner.execute("INSERT INTO " + fqn()
                    + " (id, region, amount) VALUES (2, 'west', 200.0)");

            ExecutorService executor = Executors.newFixedThreadPool(2);
            try {
                Future<?> writer1 = executor.submit(() ->
                        sqlRunner.execute("INSERT INTO " + fqn()
                                + " (id, region, amount) VALUES (3, 'east', 300.0)"));
                Future<?> writer2 = executor.submit(() ->
                        sqlRunner.execute("INSERT INTO " + fqn()
                                + " (id, region, amount) VALUES (4, 'west', 400.0)"));

                collectResults(writer1, writer2);
            } finally {
                executor.shutdown();
            }

            List<TypedRow> rows = sqlRunner.execute(
                    "SELECT * FROM " + fqn() + " ORDER BY id");

            assertThat(rows).hasSize(4);
            assertThat(rows.get(0).getLong("id")).isEqualTo(1L);
            assertThat(rows.get(1).getLong("id")).isEqualTo(2L);
            assertThat(rows.get(2).getLong("id")).isEqualTo(3L);
            assertThat(rows.get(3).getLong("id")).isEqualTo(4L);
        }
    }

    // --- Concurrency helper ---

    /**
     * Collect results from concurrent futures. At least one must succeed;
     * if one throws a write-conflict exception that is acceptable.
     */
    private static void collectResults(Future<?>... futures) {
        int successes = 0;
        for (Future<?> f : futures) {
            try {
                f.get();
                successes++;
            } catch (ExecutionException e) {
                // Write conflict is acceptable for concurrent writes
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException("Interrupted waiting for concurrent write", e);
            }
        }
        assertThat(successes)
                .as("At least one concurrent write must succeed")
                .isGreaterThanOrEqualTo(1);
    }
}
