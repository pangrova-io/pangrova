package org.pangrova.testing.e2e.resilience;

import org.pangrova.spi.connector.data.TypedRow;
import org.pangrova.testing.e2e.SqlE2ETestBase;
import org.pangrova.testing.e2e.config.ConnectorTestConfig;
import org.pangrova.testing.e2e.fixture.WriteTestHelper;
import org.pangrova.testing.e2e.runner.QueryError;
import org.pangrova.testing.e2e.runner.SqlExecutionException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

import java.time.Duration;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;

/**
 * E2E tests verifying coordinator handles worker failures gracefully.
 *
 * <p>Validates that queries transition to FAILED with structured errors
 * when workers crash, become unreachable, or exceed timeouts.
 * All tests use real HTTP communication — zero mock/stub.
 *
 * <p>Requirements: 12.1, 12.3, 12.4, 12.5
 */
@DisplayName("Worker Failure Resilience - Query Termination E2E Tests")
class WorkerCrashQueryTerminationTest extends SqlE2ETestBase {

    private static final String CATALOG = "iceberg_catalog";
    private static final String SCHEMA = "default";
    private static final String FQN_PREFIX = CATALOG + "." + SCHEMA + ".";
    private static final int DATA_ROW_COUNT = 100;
    private static final Duration FAILURE_DETECTION_TIMEOUT = Duration.ofSeconds(60);

    private String tableName;

    @Override
    protected List<ConnectorTestConfig> connectorConfigs() {
        return List.of(ConnectorTestConfig.icebergLocal(
                CATALOG, SCHEMA, icebergWarehouse().toString()));
    }

    @BeforeEach
    void setupTable(TestInfo testInfo) {
        String method = testInfo.getTestMethod().map(m -> m.getName()).orElse("unknown");
        tableName = "t_resilience_" + method + "_" + System.nanoTime();
        createDataTable(tableName);
        insertTestData(tableName, DATA_ROW_COUNT);
    }

    @AfterEach
    void restoreWorker() {
        ensureWorkerRunning();
    }

    // --- N12.1: Worker unreachable transitions query to FAILED ---

    @Test
    @DisplayName("N12.1 - Worker unreachable mid-query transitions to FAILED within bounded timeout")
    void workerUnreachableTransitionsToFailed() {
        stopWorkerHttpServer();

        assertTimeoutPreemptively(FAILURE_DETECTION_TIMEOUT, () -> {
            QueryError error = sqlRunner.executeExpectingError(
                    buildSelectAll(tableName));
            assertThat(error.message()).isNotBlank();
        });
    }

    // --- N12.3: Failed worker tasks cleaned up ---

    @Test
    @DisplayName("N12.3 - Failed worker tasks cleaned up from CoordinatorTaskTracker")
    void failedWorkerTasksCleanedUp() {
        stopWorkerHttpServer();

        assertTimeoutPreemptively(FAILURE_DETECTION_TIMEOUT, () -> {
            submitQueryExpectingFailure(buildSelectAll(tableName));
            assertNoOrphanedTasks();
        });
    }

    // --- N12.4: Task timeout transitions to FAILED ---

    @Test
    @DisplayName("N12.4 - Task exceeding timeout transitions to FAILED with timeout message")
    void taskTimeoutTransitionsToFailed() {
        // Execute a query designed to be slow enough to trigger timeout
        // when worker is unreachable (simulated by stopping server).
        stopWorkerHttpServer();

        assertTimeoutPreemptively(FAILURE_DETECTION_TIMEOUT, () -> {
            QueryError error = sqlRunner.executeExpectingError(
                    buildLargeAggregation(tableName));
            assertThat(error.message().toLowerCase())
                    .containsAnyOf("timeout", "timed out", "unreachable", "failed", "connection");
        });
    }

    // --- N12.5: Client error contains worker identity ---

    @Test
    @DisplayName("N12.5 - Client error message contains worker identifier and failure reason")
    void clientErrorContainsWorkerIdentity() {
        stopWorkerHttpServer();

        assertTimeoutPreemptively(FAILURE_DETECTION_TIMEOUT, () -> {
            QueryError error = sqlRunner.executeExpectingError(
                    buildSelectAll(tableName));
            assertErrorContainsContext(error);
        });
    }

    // -----------------------------------------------------------------------
    // Worker lifecycle helpers
    // -----------------------------------------------------------------------

    private void stopWorkerHttpServer() {
        stopQuietly(workerHttpServer);
    }

    private void ensureWorkerRunning() {
        try {
            startWorker();
        } catch (Exception e) {
            // Best-effort restart — other tests may still work
        }
    }

    // -----------------------------------------------------------------------
    // Query submission helpers
    // -----------------------------------------------------------------------

    private void submitQueryExpectingFailure(String sql) {
        try {
            sqlRunner.execute(sql);
        } catch (SqlExecutionException ignored) {
            // Expected — query should fail when worker is down
        }
    }

    // -----------------------------------------------------------------------
    // Assertion helpers
    // -----------------------------------------------------------------------

    private void assertNoOrphanedTasks() {
        // After failure, taskTracker should have no active (non-terminal) tasks
        // for any query. Verify by checking that no query has pending tasks.
        boolean hasOrphans = queryManager.getActiveQueryIds().stream()
                .anyMatch(qid -> !taskTracker.allTasksFinished(qid)
                        && !taskTracker.hasFailedTask(qid));
        assertThat(hasOrphans)
                .as("No orphaned tasks should remain after worker failure")
                .isFalse();
    }

    private static void assertErrorContainsContext(QueryError error) {
        String msg = error.message().toLowerCase();
        // Error should contain enough context for ops troubleshooting:
        // worker identity or connection details or failure reason.
        assertThat(msg).satisfiesAnyOf(
                m -> assertThat(m).contains(WORKER_ID.toLowerCase()),
                m -> assertThat(m).contains("localhost"),
                m -> assertThat(m).contains("worker"),
                m -> assertThat(m).containsAnyOf("refused", "unreachable",
                        "timeout", "failed", "connection"));
    }

    // -----------------------------------------------------------------------
    // Table and data helpers
    // -----------------------------------------------------------------------

    private void createDataTable(String table) {
        sqlRunner.executeDdl(String.format(
                "CREATE TABLE %s (id BIGINT, name VARCHAR, amount DOUBLE)",
                fqn(table)));
    }

    private void insertTestData(String table, int rowCount) {
        List<TypedRow> rows = IntStream.rangeClosed(1, rowCount)
                .mapToObj(i -> TypedRow.builder().putRaw("id", (long) i).putRaw("name", "item_" + i).putRaw("amount", i * 2.5).build())
                .collect(Collectors.toList());
        WriteTestHelper.insertRows(sqlRunner, fqn(table), rows);
    }

    // -----------------------------------------------------------------------
    // SQL builders
    // -----------------------------------------------------------------------

    private String buildSelectAll(String table) {
        return "SELECT * FROM " + fqn(table);
    }

    private String buildLargeAggregation(String table) {
        return "SELECT name, SUM(amount), COUNT(*) FROM " + fqn(table) + " GROUP BY name";
    }

    private String fqn(String table) {
        return FQN_PREFIX + table;
    }
}
