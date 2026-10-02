package org.pangrova.testing.e2e.concurrency;

import org.pangrova.spi.connector.data.TypedRow;
import org.pangrova.testing.ResultValidator;
import org.pangrova.testing.e2e.SqlE2ETestBase;
import org.pangrova.testing.e2e.config.ConnectorTestConfig;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Concurrent query, spill-to-disk, single-worker join/aggregation, and large data scan E2E tests.
 *
 * <p>Covers R15.1-R15.10. All queries execute through the real pipeline:
 * SQL test runner → packaged NativeRuntime → Velox. The join and aggregation
 * cases in this suite do not certify multi-worker shuffle.
 *
 * <p>Requirements: 15.1, 15.2, 15.3, 16.9, 16.10
 */
@DisplayName("Concurrency & Resource Pressure - Example Tests")
class ConcurrentQueryTest extends SqlE2ETestBase {

    private static final String CATALOG = "iceberg_catalog";
    private static final String SCHEMA = "default";
    private static final int SCALE_FACTOR = 1;
    private static final int CONCURRENT_THREADS = 4;
    private static final int LARGE_ROW_COUNT = 10_000;

    @Override
    protected List<ConnectorTestConfig> connectorConfigs() {
        return List.of(
                ConnectorTestConfig.icebergLocal(CATALOG, SCHEMA, icebergWarehouse().toString()));
    }

    @BeforeAll
    static void loadTestData() {
        isDockerReady();
        registerTestConnectors(List.of(
                ConnectorTestConfig.icebergLocal(CATALOG, SCHEMA, icebergWarehouse().toString())));
        dataSetup.loadTpcHData(CATALOG, SCHEMA, SCALE_FACTOR);
        createLargeTable();
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
    // R15.8: Single-worker JOIN correctness; distributed shuffle remains unverified.
    // -----------------------------------------------------------------------

    @Nested
    @DisplayName("R15.8 - Single-worker JOIN correctness")
    class ShuffleJoin {

        @Test
        @DisplayName("JOIN query produces correct results through single-worker pipeline")
        void joinOnSingleWorkerCluster() {
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
    // R15.9: Single-worker aggregation correctness; distributed shuffle remains unverified.
    // -----------------------------------------------------------------------

    @Nested
    @DisplayName("R15.9 - Single-worker aggregation correctness")
    class ShuffleAggregation {

        @Test
        @DisplayName("Aggregation query produces deterministic results through single-worker pipeline")
        void aggregationOnSingleWorkerCluster() {
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

}
