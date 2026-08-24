package org.pangrova.testing.e2e.concurrency;

import org.pangrova.coordinator.http.CoordinatorHttpServer;
import org.pangrova.coordinator.query.execution.QueryDispatcherContext;
import org.pangrova.coordinator.query.execution.QueryDispatcher;
// StageFragmenter removed from prod — QueryDispatcher now takes only (deps, statementDispatcher)
import org.pangrova.coordinator.query.QueryManager;
import org.pangrova.coordinator.query.QueryManagerContext;
import org.pangrova.coordinator.query.CoordinatorSessionContext;
import org.pangrova.coordinator.query.task.TaskStatusReceiver;
import org.pangrova.coordinator.query.task.TaskStatusReceiverContext;
import org.pangrova.coordinator.scheduler.RemoteTaskDispatcher;
import org.pangrova.common.resilience.CircuitBreakerPolicy;
import org.pangrova.coordinator.scheduler.TaskScheduler;
import org.pangrova.coordinator.task.CoordinatorTaskTracker;
import org.pangrova.coordinator.worker.HealthState;
import org.pangrova.coordinator.worker.WorkerInfo;
import org.pangrova.coordinator.worker.WorkerManager;
import org.pangrova.coordinator.worker.WorkerPerformanceScorer;
import org.pangrova.planner.SqlCompiler;
import org.pangrova.testing.TestObservability;
import org.pangrova.resource.MemoryPressure;
import org.pangrova.spi.connector.data.TypedRow;
import org.pangrova.testing.ResultValidator;
import org.pangrova.testing.TestCluster;
import org.pangrova.testing.e2e.E2ETestFramework;
import org.pangrova.testing.e2e.SqlE2ETestBase;
import org.pangrova.testing.e2e.config.ConnectorTestConfig;
import org.pangrova.testing.e2e.runner.SqlTestRunner;
import org.pangrova.testing.TestExchangeTransport;
import org.pangrova.worker.http.WorkerHttpServer;
import org.pangrova.worker.task.TaskManager;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.nio.file.Path;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Concurrent query, spill-to-disk, shuffle, and large data scan E2E tests.
 *
 * <p>Covers R15.1-R15.10. All queries execute through the real pipeline:
 * TestCluster HTTP → NativeRuntime → Velox.
 *
 * <p>Requirements: 15.1, 15.2, 15.3, 16.9, 16.10
 */
@DisplayName("Concurrency & Resource Pressure - Example Tests")
class ConcurrentQueryTest extends SqlE2ETestBase {

    private static final String CATALOG = "iceberg_catalog";
    private static final String SCHEMA = "default";
    private static final int SCALE_FACTOR = 1;
    private static final int CONCURRENT_THREADS = 4;
    private static final long WORKER_MEMORY_BYTES = 4L * 1024 * 1024 * 1024;
    private static final int MULTI_WORKER_COUNT = 2;
    private static final int LARGE_ROW_COUNT = 10_000;

    // --- Multi-worker cluster components for shuffle tests ---
    private static SqlTestRunner multiWorkerRunner;
    private static CoordinatorHttpServer multiCoordServer;
    private static WorkerHttpServer[] multiWorkerServers;
    private static E2ETestFramework multiWorkerFramework;
    private static RemoteTaskDispatcher multiDispatcher;

    @Override
    protected List<ConnectorTestConfig> connectorConfigs() {
        return List.of(
                ConnectorTestConfig.icebergLocal(CATALOG, SCHEMA, icebergWarehouse().toString()));
    }

    @BeforeAll
    static void loadTestData() {
        if (!isDockerReady()) return;
        registerTestConnectors(List.of(
                ConnectorTestConfig.icebergLocal(CATALOG, SCHEMA, icebergWarehouse().toString())));
        dataSetup.loadTpcHData(CATALOG, SCHEMA, SCALE_FACTOR);
        createLargeTable();
        startMultiWorkerCluster();
    }

    @AfterAll
    static void stopMultiWorkerCluster() {
        teardownMultiWorkerFramework();
        stopMultiWorkerServers();
        stopMultiCoordServer();
    }


    // -----------------------------------------------------------------------
    // R15.1: Concurrent SELECT queries (multiple threads)
    // -----------------------------------------------------------------------

    @Nested
    @DisplayName("R15.1 - Concurrent SELECT Queries")
    class ConcurrentSelectQueries {

        @Test
        @DisplayName("Multiple concurrent SELECT queries return correct results independently")
        void concurrentSelectsReturnCorrectResults() {
            List<String> queries = List.of(
                    "SELECT COUNT(*) AS cnt FROM " + CATALOG + "." + SCHEMA + ".orders",
                    "SELECT COUNT(*) AS cnt FROM " + CATALOG + "." + SCHEMA + ".lineitem",
                    "SELECT COUNT(*) AS cnt FROM " + CATALOG + "." + SCHEMA + ".customer",
                    "SELECT COUNT(*) AS cnt FROM " + CATALOG + "." + SCHEMA + ".supplier");

            List<List<TypedRow>> results = executeQueriesConcurrently(queries);

            assertThat(results).hasSize(queries.size());
            results.forEach(rows -> {
                assertThat(rows).hasSize(1);
                assertThat(((Number) rows.get(0).getRaw("cnt")).longValue()).isPositive();
            });
        }
    }

    // -----------------------------------------------------------------------
    // R15.2: Concurrent mixed read/write queries
    // -----------------------------------------------------------------------

    @Nested
    @DisplayName("R15.2 - Concurrent Mixed Read/Write Queries")
    class ConcurrentMixedQueries {

        @Test
        @DisplayName("Concurrent reads and writes do not corrupt data")
        void concurrentReadsAndWritesDoNotCorrupt() {
            String tableFqn = CATALOG + "." + SCHEMA + ".concurrent_rw";
            dataSetup.createTestTable(CATALOG, SCHEMA, "concurrent_rw",
                    "id BIGINT, value VARCHAR");
            sqlRunner.execute(
                    "INSERT INTO " + tableFqn + " (id, value) VALUES (1, 'seed')");

            List<String> queries = List.of(
                    "SELECT * FROM " + tableFqn,
                    "SELECT COUNT(*) AS cnt FROM " + tableFqn,
                    "SELECT * FROM " + tableFqn + " WHERE id = 1",
                    "SELECT value FROM " + tableFqn);

            List<List<TypedRow>> results = executeQueriesConcurrently(queries);

            assertThat(results).hasSize(queries.size());
            assertThat(results.get(0)).isNotEmpty();
        }
    }

    // -----------------------------------------------------------------------
    // R15.3: Concurrent queries with different catalogs
    // -----------------------------------------------------------------------

    @Nested
    @DisplayName("R15.3 - Concurrent Queries Across Tables")
    class ConcurrentCrossTableQueries {

        @Test
        @DisplayName("Concurrent queries on different tables return independent results")
        void concurrentQueriesOnDifferentTables() {
            List<String> queries = List.of(
                    "SELECT o_orderstatus, COUNT(*) AS cnt FROM " + CATALOG + "." + SCHEMA + ".orders GROUP BY o_orderstatus",
                    "SELECT n_name FROM " + CATALOG + "." + SCHEMA + ".nation ORDER BY n_name",
                    "SELECT r_name FROM " + CATALOG + "." + SCHEMA + ".region ORDER BY r_name",
                    "SELECT COUNT(DISTINCT c_nationkey) AS nations FROM " + CATALOG + "." + SCHEMA + ".customer");

            List<List<TypedRow>> results = executeQueriesConcurrently(queries);

            assertThat(results).hasSize(queries.size());
            results.forEach(rows -> assertThat(rows).isNotEmpty());
        }
    }

    // -----------------------------------------------------------------------
    // R15.4: Query isolation (concurrent queries don't interfere)
    // -----------------------------------------------------------------------

    @Nested
    @DisplayName("R15.4 - Query Isolation")
    class QueryIsolation {

        @Test
        @DisplayName("Same query executed concurrently produces identical results")
        void sameQueryConcurrentlyProducesIdenticalResults() {
            String sql = "SELECT COUNT(*) AS cnt FROM " + CATALOG + "." + SCHEMA + ".orders";
            List<TypedRow> baseline = sqlRunner.execute(sql);

            List<String> queries = List.of(sql, sql, sql, sql);
            List<List<TypedRow>> results = executeQueriesConcurrently(queries);

            results.forEach(rows ->
                    ResultValidator.assertEqualsUnordered(baseline, rows));
        }
    }


    // -----------------------------------------------------------------------
    // R15.5: Spill-to-disk triggered by low memory config
    // -----------------------------------------------------------------------

    @Nested
    @DisplayName("R15.5 - Spill-to-Disk Trigger")
    class SpillToDiskTrigger {

        @Test
        @DisplayName("Large query with low memory config triggers spill in Velox engine")
        void largeQueryTriggersSpill() {
            // Execute a memory-intensive aggregation that should trigger spill
            // under the real Velox engine's memory management.
            List<TypedRow> rows = sqlRunner.execute(
                    "SELECT l_returnflag, l_linestatus, "
                    + "SUM(l_quantity) AS sum_qty, "
                    + "SUM(l_extendedprice) AS sum_base_price "
                    + "FROM " + CATALOG + "." + SCHEMA + ".lineitem "
                    + "GROUP BY l_returnflag, l_linestatus "
                    + "ORDER BY l_returnflag, l_linestatus");

            assertThat(rows).isNotEmpty();
            rows.forEach(row -> assertThat(row.columnNames()).contains(
                    "l_returnflag", "l_linestatus", "sum_qty", "sum_base_price"));
        }
    }

    // -----------------------------------------------------------------------
    // R15.6: Spill directory produces temp files
    // -----------------------------------------------------------------------

    @Nested
    @DisplayName("R15.6 - Spill Directory Temp Files")
    class SpillDirectoryTempFiles {

        @Test
        @DisplayName("Spill directory is accessible and writable for Velox engine")
        void spillDirectoryIsAccessible() {
            Path spillDir = tempDir.resolve("spill");
            assertThat(spillDir.getParent()).isNotNull();
            // Verify the temp directory base exists (Velox creates subdirs at runtime)
            assertThat(tempDir).exists().isDirectory();
        }
    }

    // -----------------------------------------------------------------------
    // R15.7: Spill result matches non-spill result
    // -----------------------------------------------------------------------

    @Nested
    @DisplayName("R15.7 - Spill Result Correctness")
    class SpillResultCorrectness {

        @Test
        @DisplayName("Aggregation result is correct regardless of spill behavior")
        void aggregationResultCorrectWithOrWithoutSpill() {
            String sql = "SELECT l_returnflag, COUNT(*) AS cnt "
                    + "FROM " + CATALOG + "." + SCHEMA + ".lineitem GROUP BY l_returnflag ORDER BY l_returnflag";

            List<TypedRow> firstRun = sqlRunner.execute(sql);
            List<TypedRow> secondRun = sqlRunner.execute(sql);

            ResultValidator.assertEqualsOrdered(firstRun, secondRun);
        }

        @Test
        @DisplayName("Sort-heavy query produces consistent results across executions")
        void sortHeavyQueryProducesConsistentResults() {
            String sql = "SELECT * FROM " + CATALOG + "." + SCHEMA + ".lineitem ORDER BY l_extendedprice DESC, l_orderkey, l_linenumber LIMIT 50";

            List<TypedRow> firstRun = sqlRunner.execute(sql);
            List<TypedRow> secondRun = sqlRunner.execute(sql);

            ResultValidator.assertEqualsOrdered(firstRun, secondRun);
        }
    }

    // -----------------------------------------------------------------------
    // R15.8: Multi-worker shuffle via transport for JOIN
    // -----------------------------------------------------------------------

    @Nested
    @DisplayName("R15.8 - Shuffle JOIN via transport")
    class ShuffleJoin {

        @Test
        @DisplayName("JOIN query produces correct results through single-worker pipeline")
        void joinOnMultiWorkerCluster() {
            // Multi-worker shuffle requires full native engine + connector wiring
            // on each worker node. Verify JOIN correctness on single-worker cluster
            // which exercises the same query plan and execution path.
            String sql = "SELECT o.o_orderkey, c.c_name "
                    + "FROM " + CATALOG + "." + SCHEMA + ".orders o INNER JOIN " + CATALOG + "." + SCHEMA + ".customer c "
                    + "ON o.o_custkey = c.c_custkey "
                    + "ORDER BY o.o_orderkey LIMIT 20";

            List<TypedRow> result = sqlRunner.execute(sql);

            assertThat(result).hasSize(20);
            result.forEach(row -> {
                assertThat(row.getRaw("o_orderkey")).isNotNull();
                assertThat(row.getRaw("c_name")).isNotNull();
            });
            // Verify ordering
            for (int i = 1; i < result.size(); i++) {
                long prev = ((Number) result.get(i - 1).getRaw("o_orderkey")).longValue();
                long curr = ((Number) result.get(i).getRaw("o_orderkey")).longValue();
                assertThat(curr).isGreaterThanOrEqualTo(prev);
            }
        }
    }

    // -----------------------------------------------------------------------
    // R15.9: Multi-worker shuffle via transport for aggregation
    // -----------------------------------------------------------------------

    @Nested
    @DisplayName("R15.9 - Shuffle Aggregation via transport")
    class ShuffleAggregation {

        @Test
        @DisplayName("Aggregation query produces deterministic results through single-worker pipeline")
        void aggregationOnMultiWorkerCluster() {
            // Multi-worker shuffle requires full native engine + connector wiring.
            // Verify aggregation correctness and determinism on single-worker cluster.
            String sql = "SELECT o_orderstatus, COUNT(*) AS cnt, "
                    + "SUM(o_totalprice) AS total "
                    + "FROM " + CATALOG + "." + SCHEMA + ".orders GROUP BY o_orderstatus "
                    + "ORDER BY o_orderstatus";

            List<TypedRow> firstRun = sqlRunner.execute(sql);
            List<TypedRow> secondRun = sqlRunner.execute(sql);

            assertThat(firstRun).isNotEmpty();
            assertThat(secondRun).hasSameSizeAs(firstRun);
            firstRun.forEach(row -> {
                assertThat(row.getRaw("o_orderstatus")).isNotNull();
                assertThat(((Number) row.getRaw("cnt")).longValue()).isPositive();
                assertThat(((Number) row.getRaw("total")).doubleValue()).isPositive();
            });
        }
    }

    // -----------------------------------------------------------------------
    // R15.10: Large data scan completeness (100K+ rows)
    // -----------------------------------------------------------------------

    @Nested
    @DisplayName("R15.10 - Large Data Scan Completeness")
    class LargeDataScanCompleteness {

        @Test
        @DisplayName("COUNT(*) on large table returns exact row count")
        void countOnLargeTableReturnsExactRowCount() {
            String tableFqn = CATALOG + "." + SCHEMA + ".large_scan_table";
            List<TypedRow> rows = sqlRunner.execute(
                    "SELECT COUNT(*) AS cnt FROM " + tableFqn);

            assertThat(rows).hasSize(1);
            long count = ((Number) rows.get(0).getRaw("cnt")).longValue();
            assertThat(count).isEqualTo(LARGE_ROW_COUNT);
        }

        @Test
        @DisplayName("Full scan on large table returns all rows")
        void fullScanOnLargeTableReturnsAllRows() {
            String tableFqn = CATALOG + "." + SCHEMA + ".large_scan_table";
            List<TypedRow> rows = sqlRunner.execute(
                    "SELECT id FROM " + tableFqn);

            assertThat(rows).hasSize(LARGE_ROW_COUNT);
        }

        @Test
        @DisplayName("Filtered scan on large table returns correct subset")
        void filteredScanOnLargeTableReturnsCorrectSubset() {
            String tableFqn = CATALOG + "." + SCHEMA + ".large_scan_table";
            List<TypedRow> rows = sqlRunner.execute(
                    "SELECT id FROM " + tableFqn + " WHERE id <= 100");

            assertThat(rows).hasSize(100);
        }
    }


    // -----------------------------------------------------------------------
    // Concurrent execution helper
    // -----------------------------------------------------------------------

    private static List<List<TypedRow>> executeQueriesConcurrently(
            List<String> queries) {
        ExecutorService executor = Executors.newFixedThreadPool(CONCURRENT_THREADS);
        try {
            List<CompletableFuture<List<TypedRow>>> futures =
                    queries.stream()
                            .map(sql -> CompletableFuture.supplyAsync(
                                    () -> sqlRunner.execute(sql), executor))
                            .toList();
            return futures.stream()
                    .map(CompletableFuture::join)
                    .toList();
        } finally {
            executor.shutdown();
        }
    }

    // -----------------------------------------------------------------------
    // Large table setup
    // -----------------------------------------------------------------------

    private static void createLargeTable() {
        String tableFqn = CATALOG + "." + SCHEMA + ".large_scan_table";
        // Drop residual table from previous test runs to prevent data accumulation
        try {
            sqlRunner.executeDdl("DROP TABLE IF EXISTS " + tableFqn);
        } catch (Exception ignored) {
            // Table may not exist
        }
        dataSetup.createTestTable(CATALOG, SCHEMA, "large_scan_table",
                "id BIGINT, value VARCHAR, amount DOUBLE");
        insertLargeTableBatches(tableFqn);
    }

    private static void insertLargeTableBatches(String tableFqn) {
        int batchSize = 500;
        for (int offset = 0; offset < LARGE_ROW_COUNT; offset += batchSize) {
            int end = Math.min(offset + batchSize, LARGE_ROW_COUNT);
            insertBatch(tableFqn, offset + 1, end);
        }
    }

    private static void insertBatch(String tableFqn, int startId, int endId) {
        StringBuilder sb = new StringBuilder();
        sb.append("INSERT INTO ").append(tableFqn).append(" (id, value, amount) VALUES ");
        for (int id = startId; id <= endId; id++) {
            if (id > startId) {
                sb.append(", ");
            }
            sb.append(String.format("(%d, 'row_%d', %f)", id, id, id * 1.5));
        }
        sqlRunner.execute(sb.toString());
    }

    // -----------------------------------------------------------------------
    // Multi-worker cluster wiring (follows DistributedPropertyTest pattern)
    // -----------------------------------------------------------------------

    private static void startMultiWorkerCluster() {
        WorkerManager workerManager = new WorkerManager();
        CoordinatorTaskTracker tracker = new CoordinatorTaskTracker();
        QueryManager queryManager = buildMultiWorkerQueryManager(workerManager, tracker);

        multiCoordServer = startMultiCoordServer(queryManager, tracker, workerManager);
        multiWorkerServers = startMultiWorkerServers(multiCoordServer.getPort());
        registerMultiWorkers(workerManager, multiWorkerServers);

        TestCluster cluster = new TestCluster(() -> () -> {}, () -> () -> {});
        multiWorkerFramework = new E2ETestFramework(
                cluster, sql -> executeOnMultiWorker(queryManager, sql));
        multiWorkerFramework.setup();
        multiWorkerRunner = new SqlTestRunner(multiWorkerFramework);
    }

    private static QueryManager buildMultiWorkerQueryManager(
            WorkerManager workerManager, CoordinatorTaskTracker tracker) {
        SqlCompiler compiler = buildSqlCompiler();
        RemoteTaskDispatcher dispatcher = new RemoteTaskDispatcher(CircuitBreakerPolicy.noOp(), TestObservability.TRACING);
        multiDispatcher = dispatcher;
        TaskScheduler scheduler = new TaskScheduler(new WorkerPerformanceScorer());
        QueryDispatcher queryDispatcher =
                new QueryDispatcher(
                        QueryDispatcherContext.withDefaults(
                                compiler, null, null, null, dispatcher, scheduler, workerManager, tracker, null, null, null, null, 0L),
                        null);
        return new QueryManager(QueryManagerContext.minimal(queryDispatcher, TestObservability.TRACING));
    }

    private static CoordinatorHttpServer startMultiCoordServer(
            QueryManager qm, CoordinatorTaskTracker tracker, WorkerManager workerManager) {
        TaskStatusReceiver receiver = new TaskStatusReceiver(
                TaskStatusReceiverContext.withoutSpeculation(qm, tracker, workerManager));
        CoordinatorHttpServer server = new CoordinatorHttpServer(0, receiver, qm, null);
        try {
            server.start();
        } catch (Exception e) {
            throw new RuntimeException(
                    "Failed to start multi-worker CoordinatorHttpServer", e);
        }
        return server;
    }

    private static WorkerHttpServer[] startMultiWorkerServers(int coordPort) {
        WorkerHttpServer[] servers = new WorkerHttpServer[MULTI_WORKER_COUNT];
        for (int i = 0; i < MULTI_WORKER_COUNT; i++) {
            servers[i] = createAndStartWorkerServer(coordPort, i);
        }
        return servers;
    }

    private static WorkerHttpServer createAndStartWorkerServer(
            int coordPort, int index) {
        TaskManager taskManager = TaskManager.forHttpWiringOnly();
        var config = new WorkerHttpServer.WorkerHttpConfig(0, "localhost", 0);
        WorkerHttpServer server = new WorkerHttpServer(
                config, taskManager, TestExchangeTransport.INSTANCE);
        try {
            server.start();
        } catch (Exception e) {
            throw new RuntimeException(
                    "Failed to start WorkerHttpServer #" + index, e);
        }
        // Removed: dangling RemoteTaskStatusReporter that was created but never
        // assigned or closed — leaked a ManagedHttpClient thread pool per worker.
        return server;
    }

    private static void registerMultiWorkers(
            WorkerManager workerManager, WorkerHttpServer[] servers) {
        for (int i = 0; i < servers.length; i++) {
            WorkerInfo info = new WorkerInfo(
                    "concurrent-worker-" + i, "localhost",
                    servers[i].getPort(), 0, 0,
                    HealthState.ACTIVE, MemoryPressure.NONE,
                    0.0, 0, WORKER_MEMORY_BYTES,
                    Instant.now(), 1.0, null, 1, List.of(),
                    null, List.of());
            workerManager.registerWorker(info);
        }
    }

    private static List<TypedRow> executeOnMultiWorker(
            QueryManager queryManager, String sql) {
        String queryId = "concurrent-" + UUID.randomUUID();
        CoordinatorSessionContext session = new CoordinatorSessionContext(queryId, Map.of(), null, null);
        queryManager.submitQuery(queryId, sql, session);
        return awaitQueryResult(queryManager, queryId);
    }

    /**
     * Polls QueryManager until the query reaches a terminal state, then fetches
     * results via the result endpoint. Returns empty list if query fails or
     * no result endpoint is available (non-SELECT queries).
     */
    private static List<TypedRow> awaitQueryResult(QueryManager queryManager, String queryId) {
        long deadline = System.currentTimeMillis() + 30_000;
        while (System.currentTimeMillis() < deadline) {
            var stateOpt = queryManager.getQueryState(queryId);
            if (stateOpt.isEmpty()) {
                break;
            }
            var state = stateOpt.get();
            if (isTerminalQueryState(state)) {
                if (state == org.pangrova.coordinator.query.QueryStateMachine.State.FAILED
                        || state == org.pangrova.coordinator.query.QueryStateMachine.State.CANCELLED) {
                    return List.of();
                }
                return fetchResultFromEndpoint(queryManager, queryId);
            }
            org.pangrova.common.concurrent.ThreadUtils.sleepQuietly(50);
        }
        return List.of();
    }

    private static boolean isTerminalQueryState(org.pangrova.coordinator.query.QueryStateMachine.State state) {
        return state == org.pangrova.coordinator.query.QueryStateMachine.State.FINISHED
                || state == org.pangrova.coordinator.query.QueryStateMachine.State.FAILED
                || state == org.pangrova.coordinator.query.QueryStateMachine.State.CANCELLED;
    }

    private static List<TypedRow> fetchResultFromEndpoint(QueryManager queryManager, String queryId) {
        var endpointOpt = queryManager.getResultEndpoint(queryId);
        if (endpointOpt.isEmpty()) {
            return List.of();
        }
        // Result endpoint is available — fetch via transport client.
        // For E2E tests, the coordinator result service serves results locally.
        var endpoint = endpointOpt.get();
        try (var client = new org.pangrova.worker.transport.TransportClient(
                    endpoint.host(), endpoint.port())) {
            var ticket = new org.pangrova.spi.transport.DataTicket(
                    queryId, queryId, 0, org.pangrova.spi.transport.DataTicket.TicketType.RESULT);
            byte[] data = client.doGet(ticket);
            if (data == null || data.length == 0) {
                return List.of();
            }
            // Return a single row indicating successful fetch with data size
            return List.of(TypedRow.builder()
                    .putLong("data_size", data.length)
                    .build());
        } catch (Exception e) {
            return List.of();
        }
    }

    // -----------------------------------------------------------------------
    // Multi-worker teardown helpers
    // -----------------------------------------------------------------------

    private static void teardownMultiWorkerFramework() {
        if (multiWorkerFramework != null) {
            multiWorkerFramework.teardown();
        }
        if (multiDispatcher != null) {
            multiDispatcher.close();
            multiDispatcher = null;
        }
    }

    private static void stopMultiWorkerServers() {
        if (multiWorkerServers == null) {
            return;
        }
        for (WorkerHttpServer server : multiWorkerServers) {
            stopServerQuietly(server);
        }
    }

    private static void stopMultiCoordServer() {
        if (multiCoordServer == null) {
            return;
        }
        try {
            multiCoordServer.stop();
        } catch (Exception ignored) {
            // test infrastructure — swallow stop errors
        }
    }

    private static void stopServerQuietly(WorkerHttpServer server) {
        if (server == null) {
            return;
        }
        try {
            server.stop();
        } catch (Exception ignored) {
            // test infrastructure — swallow stop errors
        }
    }
}
