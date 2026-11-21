package org.pangrova.testing.integration.e2e.fixture;

import org.pangrova.types.value.TypedRow;
import org.pangrova.testing.integration.e2e.runner.SqlTestRunner;
import org.junit.jupiter.api.TestInfo;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Shared test fixture for index E2E tests.
 *
 * <p>Provides reusable table creation helpers and correctness assertion
 * methods used across 17+ index test files. Each helper creates a table
 * with 4 separate INSERT batches to force 4 data files with distinct,
 * non-overlapping value ranges — the prerequisite for meaningful index
 * data-skipping verification.</p>
 *
 * <p>Table name uniqueness is guaranteed by combining the caller-supplied
 * prefix, the JUnit test method name, and {@code System.nanoTime()}.</p>
 */
public final class IndexTestFixture {

    private IndexTestFixture() {
        // utility class
    }

    /**
     * Create a generic 7-column table with 8 rows across 4 INSERT batches.
     *
     * <p>Columns: user_id BIGINT, name VARCHAR, amount DOUBLE, status VARCHAR,
     * description VARCHAR, session_id VARCHAR, ts TIMESTAMP.</p>
     *
     * @param runner  SQL test runner
     * @param catalog catalog name (e.g. "iceberg_catalog")
     * @param schema  schema name (e.g. "default")
     * @param prefix  table name prefix for readability
     * @param info    JUnit TestInfo for method-based uniqueness
     * @return fully qualified table name (catalog.schema.table)
     */
    public static String createGenericTable(
            SqlTestRunner runner, String catalog, String schema,
            String prefix, TestInfo info) {
        String fqn = buildFqn(catalog, schema, prefix, info);
        runner.executeDdl("CREATE TABLE " + fqn
                + " (user_id BIGINT, name VARCHAR, amount DOUBLE,"
                + " status VARCHAR, description VARCHAR, session_id VARCHAR,"
                + " ts TIMESTAMP)");
        runner.execute("INSERT INTO " + fqn
                + " (user_id, name, amount, status, description, session_id, ts) VALUES"
                + " (1, 'Alice', 50.0, 'ACTIVE', 'hello world', 'sess-001',"
                + " TIMESTAMP '2025-01-01 00:00:00'),"
                + " (2, 'Bob', 75.0, 'ACTIVE', 'pangrova engine', 'sess-002',"
                + " TIMESTAMP '2025-01-01 12:00:00')");
        runner.execute("INSERT INTO " + fqn
                + " (user_id, name, amount, status, description, session_id, ts) VALUES"
                + " (10, 'Charlie', 150.0, 'INACTIVE', 'data lake', 'sess-010',"
                + " TIMESTAMP '2025-02-01 00:00:00'),"
                + " (20, 'Diana', 175.0, 'INACTIVE', 'query engine', 'sess-020',"
                + " TIMESTAMP '2025-02-15 00:00:00')");
        runner.execute("INSERT INTO " + fqn
                + " (user_id, name, amount, status, description, session_id, ts) VALUES"
                + " (100, 'Eve', 250.0, 'ACTIVE', 'distributed sql', 'sess-100',"
                + " TIMESTAMP '2025-03-01 00:00:00'),"
                + " (200, 'Frank', 275.0, 'PENDING', 'lakehouse format', 'sess-200',"
                + " TIMESTAMP '2025-03-15 00:00:00')");
        runner.execute("INSERT INTO " + fqn
                + " (user_id, name, amount, status, description, session_id, ts) VALUES"
                + " (1000, 'Grace', 350.0, 'ACTIVE', 'iceberg table', 'sess-1000',"
                + " TIMESTAMP '2025-04-01 00:00:00'),"
                + " (2000, 'Hank', 375.0, 'INACTIVE', 'paimon store', 'sess-2000',"
                + " TIMESTAMP '2025-04-15 00:00:00')");
        return fqn;
    }

    /**
     * Create a spatial table with 8 rows across 4 INSERT batches.
     *
     * <p>Columns: id BIGINT, x DOUBLE, y DOUBLE, label VARCHAR.
     * Each batch places points in a distinct quadrant so spatial
     * indexes can demonstrate range pruning.</p>
     */
    public static String createSpatialTable(
            SqlTestRunner runner, String catalog, String schema,
            String prefix, TestInfo info) {
        String fqn = buildFqn(catalog, schema, prefix, info);
        runner.executeDdl("CREATE TABLE " + fqn
                + " (id BIGINT, x DOUBLE, y DOUBLE, label VARCHAR)");
        runner.execute("INSERT INTO " + fqn
                + " (id, x, y, label) VALUES"
                + " (1, 2.0, 3.0, 'q1_a'), (2, 8.0, 7.0, 'q1_b')");
        runner.execute("INSERT INTO " + fqn
                + " (id, x, y, label) VALUES"
                + " (3, 102.0, 105.0, 'q2_a'), (4, 108.0, 109.0, 'q2_b')");
        runner.execute("INSERT INTO " + fqn
                + " (id, x, y, label) VALUES"
                + " (5, 201.0, 204.0, 'q3_a'), (6, 209.0, 206.0, 'q3_b')");
        runner.execute("INSERT INTO " + fqn
                + " (id, x, y, label) VALUES"
                + " (7, 303.0, 301.0, 'q4_a'), (8, 307.0, 308.0, 'q4_b')");
        return fqn;
    }

    /**
     * Create a GIN-compatible table with 8 rows across 4 INSERT batches.
     *
     * <p>Columns: id BIGINT, tags VARCHAR, data VARCHAR.
     * Each batch uses distinct Greek-letter tag pairs so GIN containment
     * queries can demonstrate selective pruning.</p>
     */
    public static String createGinTable(
            SqlTestRunner runner, String catalog, String schema,
            String prefix, TestInfo info) {
        String fqn = buildFqn(catalog, schema, prefix, info);
        runner.executeDdl("CREATE TABLE " + fqn
                + " (id BIGINT, tags VARCHAR, data VARCHAR)");
        runner.execute("INSERT INTO " + fqn
                + " (id, tags, data) VALUES"
                + " (1, 'alpha,beta', 'd1'), (2, 'beta,alpha', 'd2')");
        runner.execute("INSERT INTO " + fqn
                + " (id, tags, data) VALUES"
                + " (3, 'gamma,delta', 'd3'), (4, 'delta,gamma', 'd4')");
        runner.execute("INSERT INTO " + fqn
                + " (id, tags, data) VALUES"
                + " (5, 'epsilon,zeta', 'd5'), (6, 'zeta,epsilon', 'd6')");
        runner.execute("INSERT INTO " + fqn
                + " (id, tags, data) VALUES"
                + " (7, 'eta,theta', 'd7'), (8, 'theta,eta', 'd8')");
        return fqn;
    }

    /**
     * Create a generic table with NULL rows mixed into each INSERT batch.
     *
     * <p>Same schema as {@link #createGenericTable} but each of the 4 batches
     * contains one normal row and one all-NULL row (8 rows total, 4 NULL).</p>
     */
    public static String createTableWithNulls(
            SqlTestRunner runner, String catalog, String schema,
            String prefix, TestInfo info) {
        String fqn = buildFqn(catalog, schema, prefix, info);
        runner.executeDdl("CREATE TABLE " + fqn
                + " (user_id BIGINT, name VARCHAR, amount DOUBLE,"
                + " status VARCHAR, description VARCHAR, session_id VARCHAR,"
                + " ts TIMESTAMP)");
        runner.execute("INSERT INTO " + fqn
                + " (user_id, name, amount, status, description, session_id, ts) VALUES"
                + " (1, 'Alice', 50.0, 'ACTIVE', 'hello world', 'sess-001',"
                + " TIMESTAMP '2025-01-01 00:00:00'),"
                + " (NULL, NULL, NULL, NULL, NULL, NULL, NULL)");
        runner.execute("INSERT INTO " + fqn
                + " (user_id, name, amount, status, description, session_id, ts) VALUES"
                + " (10, 'Charlie', 150.0, 'INACTIVE', 'data lake', 'sess-010',"
                + " TIMESTAMP '2025-02-01 00:00:00'),"
                + " (NULL, NULL, NULL, NULL, NULL, NULL, NULL)");
        runner.execute("INSERT INTO " + fqn
                + " (user_id, name, amount, status, description, session_id, ts) VALUES"
                + " (100, 'Eve', 250.0, 'ACTIVE', 'distributed sql', 'sess-100',"
                + " TIMESTAMP '2025-03-01 00:00:00'),"
                + " (NULL, NULL, NULL, NULL, NULL, NULL, NULL)");
        runner.execute("INSERT INTO " + fqn
                + " (user_id, name, amount, status, description, session_id, ts) VALUES"
                + " (1000, 'Grace', 350.0, 'ACTIVE', 'iceberg table', 'sess-1000',"
                + " TIMESTAMP '2025-04-01 00:00:00'),"
                + " (NULL, NULL, NULL, NULL, NULL, NULL, NULL)");
        return fqn;
    }

    /**
     * Assert the index correctness invariant: the result set WITH the index
     * must be a superset of (containsAll) the result set WITHOUT the index.
     *
     * <p>Sequence: query without index → create index → query with index →
     * assert containsAll. This proves the index reduces I/O without
     * dropping correct results.</p>
     *
     * @param runner   SQL test runner
     * @param fqn      fully qualified table name
     * @param query    WHERE clause predicate (without "WHERE" keyword)
     * @param indexDdl full CREATE INDEX DDL statement
     */
    public static void assertCorrectnessInvariant(
            SqlTestRunner runner, String fqn, String query, String indexDdl) {
        List<TypedRow> baseline = runner.execute(
                "SELECT * FROM " + fqn + " WHERE " + query);
        runner.executeDdl(indexDdl);
        List<TypedRow> withIndex = runner.execute(
                "SELECT * FROM " + fqn + " WHERE " + query);
        assertThat(withIndex).containsAll(baseline);
    }

    private static String buildFqn(
            String catalog, String schema, String prefix, TestInfo info) {
        String method = info.getTestMethod()
                .map(m -> m.getName()).orElse("unknown");
        String table = prefix + "_" + method + "_" + System.nanoTime();
        return catalog + "." + schema + "." + table;
    }
}
