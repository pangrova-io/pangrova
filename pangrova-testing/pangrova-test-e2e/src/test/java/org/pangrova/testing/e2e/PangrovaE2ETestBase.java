package org.pangrova.testing.e2e;

import org.pangrova.testing.E2ETest;
import org.pangrova.testing.e2e.config.ConnectorTestConfig;
import org.pangrova.spi.connector.data.ColumnValue;
import org.pangrova.spi.connector.data.TypedRow;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Simplified base class for E2E scenario tests (Tasks 6-20).
 *
 * <p>Extends the existing {@link SqlE2ETestBase} infrastructure (in-JVM coordinator + worker)
 * and adds convenience methods for:
 * <ul>
 *   <li>Executing SQL statements and asserting results</li>
 *   <li>JDBC connection factory (for client-side verification)</li>
 *   <li>Common assertion patterns (row count, column values, error matching)</li>
 *   <li>Timeout-guarded execution</li>
 * </ul>
 *
 * <p>Scenario test classes should extend this base and use {@code @E2ETest}:
 * <pre>{@code
 * @E2ETest
 * class WriteDmlTransactionE2ETest extends PangrovaE2ETestBase {
 *     @Test void insertSelectRoundTrip() {
 *         executeDdl("CREATE TABLE t1 (id INT, name VARCHAR)");
 *         executeDml("INSERT INTO t1 VALUES (1, 'hello')");
 *         assertQueryReturnsRows("SELECT * FROM t1", 1);
 *     }
 * }
 * }</pre>
 */
@E2ETest
public abstract class PangrovaE2ETestBase extends SqlE2ETestBase {

    private static final Logger LOG = LoggerFactory.getLogger(PangrovaE2ETestBase.class);
    private static final long DEFAULT_TIMEOUT_SECONDS = 120;

    // -----------------------------------------------------------------------
    // SQL execution helpers
    // -----------------------------------------------------------------------

    /**
     * Execute a DDL statement (CREATE, DROP, ALTER) and assert it completes successfully.
     */
    protected void executeDdl(String sql) {
        E2ETestFramework.QueryResult result = framework.executeQuery(sql);
        assertThat(result.success())
                .as("DDL should succeed: %s — error: %s", sql, result.errorMessage())
                .isTrue();
    }

    /**
     * Execute a DML statement (INSERT, UPDATE, DELETE, MERGE) and assert success.
     */
    protected void executeDml(String sql) {
        E2ETestFramework.QueryResult result = framework.executeQuery(sql);
        assertThat(result.success())
                .as("DML should succeed: %s — error: %s", sql, result.errorMessage())
                .isTrue();
    }

    /**
     * Execute a query and return the result rows.
     */
    protected List<TypedRow> executeQuery(String sql) {
        E2ETestFramework.QueryResult result = framework.executeQuery(sql);
        assertThat(result.success())
                .as("Query should succeed: %s — error: %s", sql, result.errorMessage())
                .isTrue();
        return result.rows();
    }

    /**
     * Execute a query that is expected to fail and return the error message.
     */
    protected String executeExpectingError(String sql) {
        E2ETestFramework.QueryResult result = framework.executeQuery(sql);
        assertThat(result.success())
                .as("Query should fail: %s", sql)
                .isFalse();
        return result.errorMessage();
    }

    // -----------------------------------------------------------------------
    // Assertion helpers
    // -----------------------------------------------------------------------

    /**
     * Assert that a query returns exactly the expected number of rows.
     */
    protected void assertQueryReturnsRows(String sql, int expectedRowCount) {
        List<TypedRow> rows = executeQuery(sql);
        assertThat(rows).hasSize(expectedRowCount);
    }

    /**
     * Assert that a query returns at least one row.
     */
    protected void assertQueryReturnsNonEmpty(String sql) {
        List<TypedRow> rows = executeQuery(sql);
        assertThat(rows).isNotEmpty();
    }

    /**
     * Assert that a query returns zero rows.
     */
    protected void assertQueryReturnsEmpty(String sql) {
        List<TypedRow> rows = executeQuery(sql);
        assertThat(rows).isEmpty();
    }

    /**
     * Assert that executing a query produces an error containing the expected substring.
     */
    protected void assertQueryFailsWithMessage(String sql, String expectedErrorSubstring) {
        String error = executeExpectingError(sql);
        assertThat(error)
                .as("Error message for: %s", sql)
                .containsIgnoringCase(expectedErrorSubstring);
    }

    /**
     * Assert that a scalar query (single row, single column) returns the expected value.
     */
    protected void assertScalarEquals(String sql, Object expectedValue) {
        List<TypedRow> rows = executeQuery(sql);
        assertThat(rows).hasSize(1);
        TypedRow row = rows.get(0);
        assertThat(row.values()).isNotEmpty();
        // Get the first column value
        Object actual = ColumnValue.toRaw(row.values().values().iterator().next());
        assertThat(actual).isEqualTo(expectedValue);
    }

    // -----------------------------------------------------------------------
    // JDBC connection factory (client-side verification)
    // -----------------------------------------------------------------------

    /**
     * Creates a JDBC connection to the running coordinator.
     * Useful for testing the Pangrova JDBC driver path end-to-end.
     *
     * <p>Caller is responsible for closing the connection.
     *
     * @return a live JDBC connection
     * @throws SQLException if connection fails
     */
    protected Connection createJdbcConnection() throws SQLException {
        int port = coordinatorHttpServer.getPort();
        String url = "jdbc:pangrova://localhost:" + port;
        return DriverManager.getConnection(url);
    }

    /**
     * Execute SQL via JDBC and return all rows as a list of Object arrays.
     */
    protected List<Object[]> executeViaJdbc(String sql) throws SQLException {
        try (Connection conn = createJdbcConnection();
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {
            int colCount = rs.getMetaData().getColumnCount();
            List<Object[]> rows = new ArrayList<>();
            while (rs.next()) {
                Object[] row = new Object[colCount];
                for (int i = 0; i < colCount; i++) {
                    row[i] = rs.getObject(i + 1);
                }
                rows.add(row);
            }
            return rows;
        }
    }

    // -----------------------------------------------------------------------
    // Timeout helpers
    // -----------------------------------------------------------------------

    /**
     * Execute a query with a timeout guard.
     * Throws AssertionError if the query exceeds the given timeout.
     */
    protected List<TypedRow> executeWithTimeout(String sql, long timeoutSeconds) {
        long start = System.nanoTime();
        E2ETestFramework.QueryResult result = framework.executeQuery(sql);
        long elapsed = TimeUnit.NANOSECONDS.toSeconds(System.nanoTime() - start);
        assertThat(elapsed)
                .as("Query '%s' exceeded timeout of %ds (took %ds)", sql, timeoutSeconds, elapsed)
                .isLessThanOrEqualTo(timeoutSeconds);
        assertThat(result.success())
                .as("Query should succeed: %s — error: %s", sql, result.errorMessage())
                .isTrue();
        return result.rows();
    }

    /**
     * Wait until a condition query returns a non-empty result, polling at intervals.
     *
     * <p>F-E2E-01 fix: delegates to Awaitility's deterministic polling instead of
     * a hand-rolled {@code while + Thread.sleep} loop. Awaitility handles
     * interrupt propagation, jittered scheduling and structured failure messages,
     * eliminating the busy-wait anti-pattern flagged by CLAUDE.md C3.
     *
     * @param sql            the condition query
     * @param timeoutSeconds maximum wait time
     * @param intervalMs     polling interval in milliseconds
     */
    protected void awaitCondition(String sql, long timeoutSeconds, long intervalMs) {
        try {
            org.awaitility.Awaitility.await("e2e-condition: " + sql)
                    .atMost(java.time.Duration.ofSeconds(timeoutSeconds))
                    .pollInterval(java.time.Duration.ofMillis(intervalMs))
                    .pollDelay(java.time.Duration.ZERO)
                    .ignoreExceptions()
                    .until(() -> {
                        E2ETestFramework.QueryResult result = framework.executeQuery(sql);
                        return result.success() && !result.rows().isEmpty();
                    });
        } catch (org.awaitility.core.ConditionTimeoutException e) {
            throw new AssertionError(
                    "Condition not met within " + timeoutSeconds + "s: " + sql, e);
        }
    }

    // -----------------------------------------------------------------------
    // Connector config — default empty (subclasses override as needed)
    // -----------------------------------------------------------------------

    @Override
    protected List<ConnectorTestConfig> connectorConfigs() {
        return List.of();
    }
}
