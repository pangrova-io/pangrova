package org.pangrova.testing.integration.e2e;

import org.pangrova.spi.connector.data.value.TypedRow;
import org.pangrova.testing.testkit.TestCluster;

import java.util.List;
import java.util.Objects;

/**
 * Full query lifecycle testing framework using a managed test cluster.
 *
 * <p>Wraps a {@link TestCluster} and a {@link QueryRunner} to provide
 * a high-level API for executing SQL queries and capturing results
 * with latency and error information.
 *
 * <p>Usage:
 * <pre>{@code
 *   E2ETestFramework fw = new E2ETestFramework(cluster, runner);
 *   fw.setup();
 *   QueryResult result = fw.executeQuery("SELECT 1");
 *   fw.teardown();
 * }</pre>
 */
public final class E2ETestFramework {

    /**
     * Captures the outcome of a single SQL query execution,
     * including result rows, latency, success flag, and optional error message.
     */
    public record QueryResult(
            List<TypedRow> rows,
            long latencyMs,
            boolean success,
            String errorMessage) {

        public QueryResult {
            rows = List.copyOf(Objects.requireNonNull(rows, "rows"));
        }

        /** Convenience constructor for successful results (no error message). */
        public QueryResult(List<TypedRow> rows, long latencyMs, boolean success) {
            this(rows, latencyMs, success, null);
        }
    }

    /**
     * Functional interface for executing a SQL string and returning typed rows.
     */
    @FunctionalInterface
    public interface QueryRunner {
        /**
         * Execute the given SQL and return result rows.
         *
         * @param sql the SQL statement to execute
         * @return list of typed result rows
         */
        List<TypedRow> run(String sql);
    }

    private final TestCluster cluster;
    private final QueryRunner queryRunner;

    /**
     * Create a new E2E test framework.
     *
     * @param cluster     the test cluster to manage lifecycle
     * @param queryRunner the query execution delegate
     */
    public E2ETestFramework(TestCluster cluster, QueryRunner queryRunner) {
        this.cluster = Objects.requireNonNull(cluster, "cluster must not be null");
        this.queryRunner = Objects.requireNonNull(queryRunner, "queryRunner must not be null");
    }

    /** Start the test cluster. */
    public void setup() {
        cluster.start();
    }

    /** Stop the test cluster. */
    public void teardown() {
        cluster.stop();
    }

    /**
     * Execute a SQL query and return a structured result.
     *
     * @param sql the SQL statement to execute; must not be null
     * @return query result with rows, latency, and success/error info
     */
    public QueryResult executeQuery(String sql) {
        Objects.requireNonNull(sql, "sql must not be null");
        long start = System.currentTimeMillis();
        try {
            List<TypedRow> rows = queryRunner.run(sql);
            long latency = System.currentTimeMillis() - start;
            return new QueryResult(rows, latency, true, null);
        } catch (Exception e) {
            long latency = System.currentTimeMillis() - start;
            return new QueryResult(List.of(), latency, false, e.getMessage());
        }
    }
}
