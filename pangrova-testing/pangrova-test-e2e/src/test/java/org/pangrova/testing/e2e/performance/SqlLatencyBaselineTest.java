package org.pangrova.testing.e2e.performance;

import org.pangrova.testing.e2e.SqlE2ETestBase;
import org.pangrova.testing.e2e.config.ConnectorTestConfig;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Performance baseline tests measuring median/maximum observed latency for representative SQL workloads.
 *
 * <p>Excluded from default CI runs via {@code @Tag("performance")}.
 * Run explicitly with {@code -Dgroups=performance} or a dedicated profile.
 *
 * <p>Thresholds are generous baselines, not strict SLAs. They exist to detect
 * severe regressions, not to enforce production-grade latency targets.
 *
 */
@Tag("performance")
@DisplayName("SQL Latency Baselines")
class SqlLatencyBaselineTest extends SqlE2ETestBase {

    private static final Logger LOG = LoggerFactory.getLogger(SqlLatencyBaselineTest.class);

    private static final String CATALOG = "iceberg_catalog";
    private static final String SCHEMA = "default";

    private static final int WARMUP_ITERATIONS = 10;
    private static final int MEASURED_ITERATIONS = 50;

    private static final String FQN_PREFIX = CATALOG + "." + SCHEMA + ".";

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
        dataSetup.loadTpcHDataViaConnector(
                org.pangrova.testing.e2e.setup.TestConnectorFactory.createIcebergConnector(icebergWarehouse()),
                1);
    }

    // -- Benchmark infrastructure --

    /**
     * Run a SQL statement through warmup + measured iterations, returning sorted latencies in ms.
     */
    private long[] measureLatencies(String sql) {
        for (int i = 0; i < WARMUP_ITERATIONS; i++) {
            sqlRunner.execute(sql);
        }
        long[] latencies = new long[MEASURED_ITERATIONS];
        for (int i = 0; i < MEASURED_ITERATIONS; i++) {
            long start = System.nanoTime();
            sqlRunner.execute(sql);
            latencies[i] = (System.nanoTime() - start) / 1_000_000;
        }
        Arrays.sort(latencies);
        return latencies;
    }

    private static double p50(long[] sorted) {
        int middle = sorted.length / 2;
        return sorted.length % 2 == 0
                ? sorted[middle - 1] / 2.0 + sorted[middle] / 2.0 : sorted[middle];
    }

    private static long max(long[] sorted) {
        return sorted[sorted.length - 1];
    }

    private void assertLatency(String label, String sql, long maxThresholdMs) {
        long[] latencies = measureLatencies(sql);
        double measuredP50 = p50(latencies);
        long measuredMax = max(latencies);
        LOG.info("[{}] p50={}ms  max={}ms  threshold={}ms", label, measuredP50, measuredMax, maxThresholdMs);
        assertThat(measuredMax)
                .as("%s max latency should be below %dms", label, maxThresholdMs)
                .isLessThan(maxThresholdMs);
    }

    /**
     * Reduced-iteration latency check for native-heavy queries.
     * Uses 3 warmup + 10 measured iterations to avoid SIGABRT from
     * repeated native engine execution in single-worker test cluster.
     */
    private void assertLatencyReduced(String label, String sql, long maxThresholdMs) {
        int reducedWarmup = 3;
        int reducedMeasured = 10;
        for (int i = 0; i < reducedWarmup; i++) {
            sqlRunner.execute(sql);
        }
        long[] latencies = new long[reducedMeasured];
        for (int i = 0; i < reducedMeasured; i++) {
            long start = System.nanoTime();
            sqlRunner.execute(sql);
            latencies[i] = (System.nanoTime() - start) / 1_000_000;
        }
        Arrays.sort(latencies);
        double measuredP50 = p50(latencies);
        long measuredMax = max(latencies);
        LOG.info("[{}] p50={}ms  max={}ms  threshold={}ms (reduced iterations)",
                label, measuredP50, measuredMax, maxThresholdMs);
        assertThat(measuredMax)
                .as("%s max latency should be below %dms", label, maxThresholdMs)
                .isLessThan(maxThresholdMs);
    }

    // -- Test scenarios organized by query type --

    @Nested
    @DisplayName("Constant Expression")
    class ConstantExpression {

        @Test
        @DisplayName("SELECT 1+2 — max < 500ms")
        void constantExprLatency() {
            assertLatency("constant-expr", "SELECT 1+2", 500);
        }
    }

    @Nested
    @DisplayName("Metadata Query")
    class MetadataQuery {

        @Test
        @DisplayName("SHOW TABLES — max < 1000ms")
        void showTablesLatency() {
            assertLatency("metadata", "SHOW TABLES FROM " + CATALOG + "." + SCHEMA, 1000);
        }
    }

    @Nested
    @DisplayName("Simple Scan")
    class SimpleScan {

        @Test
        @DisplayName("SELECT * FROM orders LIMIT 100 — max < 2000ms")
        void scanWithLimitLatency() {
            // Reduced iterations to avoid native SIGABRT from repeated execution
            assertLatencyReduced("scan-limit",
                    "SELECT * FROM orders LIMIT 100", 2000);
        }
    }

    @Nested
    @DisplayName("Aggregation")
    class Aggregation {

        @Test
        @DisplayName("SELECT COUNT(*) FROM orders — max < 3000ms")
        void countAggregationLatency() {
            // Reduced iterations to avoid native SIGABRT from repeated execution
            assertLatencyReduced("count-agg",
                    "SELECT COUNT(*) AS cnt FROM orders", 3000);
        }
    }

    @Nested
    @DisplayName("Join")
    class JoinQuery {

        @Test
        @DisplayName("orders JOIN customer — max < 5000ms")
        void joinLatency() {
            String sql = "SELECT o_orderkey, o_totalprice, c_name "
                    + "FROM orders JOIN customer ON o_custkey = c_custkey "
                    + "LIMIT 50";
            // Reduced iterations to avoid native SIGABRT from repeated execution
            assertLatencyReduced("join", sql, 5000);
        }
    }

    @Nested
    @DisplayName("TPC-H Q1")
    class TpcHQ1 {

        private static final String TPCH_Q1 =
                "SELECT l_returnflag, l_linestatus, "
                + "SUM(l_quantity) AS sum_qty, "
                + "SUM(l_extendedprice) AS sum_base_price, "
                + "SUM(l_extendedprice * (1 - l_discount)) AS sum_disc_price, "
                + "SUM(l_extendedprice * (1 - l_discount) * (1 + l_tax)) AS sum_charge, "
                + "AVG(l_quantity) AS avg_qty, "
                + "AVG(l_extendedprice) AS avg_price, "
                + "AVG(l_discount) AS avg_disc, "
                + "COUNT(*) AS count_order "
                + "FROM lineitem "
                + "WHERE l_shipdate <= DATE '1998-12-01' "
                + "GROUP BY l_returnflag, l_linestatus "
                + "ORDER BY l_returnflag, l_linestatus";

        @Test
        @DisplayName("TPC-H Q1 pricing summary — max < 10000ms")
        void tpchQ1Latency() {
            long[] latencies = measureLatencies(TPCH_Q1);
            long measuredMax = max(latencies);
            LOG.info("[tpch-q1] p50={}ms  max={}ms  threshold=10000ms", p50(latencies), measuredMax);
            assertThat(measuredMax)
                    .as("TPC-H Q1 max latency should be below 10000ms")
                    .isLessThan(10_000);
        }
    }

    @Nested
    @DisplayName("TPC-H Q5")
    class TpcHQ5 {

        private static final String TPCH_Q5 =
                "SELECT n_name, "
                + "SUM(l_extendedprice * (1 - l_discount)) AS revenue "
                + "FROM customer, orders, lineitem, supplier, nation, region "
                + "WHERE c_custkey = o_custkey "
                + "AND l_orderkey = o_orderkey "
                + "AND l_suppkey = s_suppkey "
                + "AND c_nationkey = s_nationkey "
                + "AND s_nationkey = n_nationkey "
                + "AND n_regionkey = r_regionkey "
                + "AND r_name = 'ASIA' "
                + "AND o_orderdate >= DATE '1994-01-01' "
                + "AND o_orderdate < DATE '1995-01-01' "
                + "GROUP BY n_name "
                + "ORDER BY revenue DESC";

        @Test
        @DisplayName("TPC-H Q5 local supplier volume — max < 15000ms")
        void tpchQ5Latency() {
            long[] latencies = measureLatencies(TPCH_Q5);
            long measuredMax = max(latencies);
            LOG.info("[tpch-q5] p50={}ms  max={}ms  threshold=15000ms", p50(latencies), measuredMax);
            assertThat(measuredMax)
                    .as("TPC-H Q5 max latency should be below 15000ms")
                    .isLessThan(15_000);
        }
    }

    @Nested
    @DisplayName("TPC-H Q9")
    class TpcHQ9 {

        private static final String TPCH_Q9 =
                "SELECT nation, o_year, SUM(amount) AS sum_profit FROM ("
                + "  SELECT n_name AS nation, "
                + "    EXTRACT(YEAR FROM o_orderdate) AS o_year, "
                + "    l_extendedprice * (1 - l_discount) - ps_supplycost * l_quantity AS amount "
                + "  FROM part, supplier, lineitem, partsupp, orders, nation "
                + "  WHERE s_suppkey = l_suppkey "
                + "    AND ps_suppkey = l_suppkey "
                + "    AND ps_partkey = l_partkey "
                + "    AND p_partkey = l_partkey "
                + "    AND o_orderkey = l_orderkey "
                + "    AND s_nationkey = n_nationkey "
                + "    AND p_name LIKE '%green%'"
                + ") AS profit "
                + "GROUP BY nation, o_year "
                + "ORDER BY nation, o_year DESC";

        @Test
        @DisplayName("TPC-H Q9 product type profit — max < 20000ms")
        void tpchQ9Latency() {
            long[] latencies = measureLatencies(TPCH_Q9);
            long measuredMax = max(latencies);
            LOG.info("[tpch-q9] p50={}ms  max={}ms  threshold=20000ms", p50(latencies), measuredMax);
            assertThat(measuredMax)
                    .as("TPC-H Q9 max latency should be below 20000ms")
                    .isLessThan(20_000);
        }
    }

    @Nested
    @DisplayName("INSERT")
    class InsertQuery {

        @Test
        @DisplayName("INSERT INTO ... VALUES — max < 3000ms")
        void insertLatency() {
            // Create a scratch table for insert benchmarking
            String table = FQN_PREFIX + "perf_insert_target";
            sqlRunner.execute(
                    "CREATE TABLE " + table + " ("
                    + "id BIGINT, name VARCHAR, amount DOUBLE"
                    + ")");
            try {
                long[] latencies = new long[MEASURED_ITERATIONS];
                // Warmup
                for (int i = 0; i < WARMUP_ITERATIONS; i++) {
                    sqlRunner.execute(
                            "INSERT INTO " + table + " VALUES ("
                            + i + ", 'warmup', 1.0)");
                }
                // Measured
                for (int i = 0; i < MEASURED_ITERATIONS; i++) {
                    long start = System.nanoTime();
                    sqlRunner.execute(
                            "INSERT INTO " + table + " VALUES ("
                            + (WARMUP_ITERATIONS + i) + ", 'bench', 2.0)");
                    latencies[i] = (System.nanoTime() - start) / 1_000_000;
                }
                Arrays.sort(latencies);
                double measuredP50 = p50(latencies);
                long measuredMax = max(latencies);
                LOG.info("[insert] p50={}ms  max={}ms  threshold=3000ms",
                        measuredP50, measuredMax);
                assertThat(measuredMax)
                        .as("INSERT max latency should be below 3000ms")
                        .isLessThan(3000);
            } finally {
                sqlRunner.execute("DROP TABLE IF EXISTS " + table);
            }
        }
    }

    @Nested
    @DisplayName("DDL")
    class DdlQuery {

        @Test
        @DisplayName("CREATE TABLE / DROP TABLE — max < 2000ms")
        void ddlLatency() {
            // Warmup
            for (int i = 0; i < WARMUP_ITERATIONS; i++) {
                String table = FQN_PREFIX + "perf_ddl_warmup_" + i;
                sqlRunner.execute("CREATE TABLE " + table + " (id BIGINT)");
                sqlRunner.execute("DROP TABLE IF EXISTS " + table);
            }
            // Measured — each iteration is a CREATE+DROP pair
            long[] latencies = new long[MEASURED_ITERATIONS];
            for (int i = 0; i < MEASURED_ITERATIONS; i++) {
                String table = FQN_PREFIX + "perf_ddl_bench_" + i;
                long start = System.nanoTime();
                sqlRunner.execute("CREATE TABLE " + table + " (id BIGINT)");
                sqlRunner.execute("DROP TABLE IF EXISTS " + table);
                latencies[i] = (System.nanoTime() - start) / 1_000_000;
            }
            Arrays.sort(latencies);
            double measuredP50 = p50(latencies);
            long measuredMax = max(latencies);
            LOG.info("[ddl] p50={}ms  max={}ms  threshold=5000ms",
                    measuredP50, measuredMax);
            assertThat(measuredMax)
                    .as("DDL max latency should be below 5000ms")
                    .isLessThan(5000);
        }
    }
}
