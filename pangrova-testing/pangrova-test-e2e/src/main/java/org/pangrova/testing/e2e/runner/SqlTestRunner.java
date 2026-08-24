package org.pangrova.testing.e2e.runner;

import org.pangrova.spi.connector.data.TypedRow;
import org.pangrova.testing.e2e.E2ETestFramework;

import java.util.List;
import java.util.Objects;

/**
 * SQL execution wrapper for E2E tests.
 *
 * <p>Delegates to {@link E2ETestFramework} and provides a test-oriented API
 * with structured error handling. Supports three execution modes:
 * <ul>
 *   <li>{@link #execute(String)} — execute and return rows, throw on failure</li>
 *   <li>{@link #executeExpectingError(String)} — execute expecting failure, return error info</li>
 *   <li>{@link #executeDdl(String)} — execute DDL with no result rows expected</li>
 * </ul>
 */
public final class SqlTestRunner {

    private final E2ETestFramework framework;

    /**
     * Create a new SQL test runner.
     *
     * @param framework the E2E test framework to delegate to; must not be null
     */
    public SqlTestRunner(E2ETestFramework framework) {
        this.framework = Objects.requireNonNull(framework, "framework must not be null");
    }

    /**
     * Execute SQL and return typed result rows. Throws on failure.
     *
     * @param sql the SQL statement to execute; must not be null
     * @return list of typed result rows
     * @throws SqlExecutionException if the query fails
     */
    public List<TypedRow> execute(String sql) {
        Objects.requireNonNull(sql, "sql must not be null");
        E2ETestFramework.QueryResult result = framework.executeQuery(sql);
        if (!result.success()) {
            throw new SqlExecutionException(sql, result);
        }
        return result.rows();
    }

    /**
     * Execute SQL expecting failure, return structured error info.
     *
     * @param sql the SQL statement expected to fail; must not be null
     * @return structured error information from the failed query
     * @throws AssertionError if the query succeeds unexpectedly
     */
    public QueryError executeExpectingError(String sql) {
        Objects.requireNonNull(sql, "sql must not be null");
        E2ETestFramework.QueryResult result = framework.executeQuery(sql);
        if (result.success()) {
            throw new AssertionError("Expected error but query succeeded: " + sql);
        }
        return QueryError.from(result);
    }

    /**
     * Execute DDL statement (no result rows expected).
     *
     * @param sql the DDL statement to execute; must not be null
     */
    public void executeDdl(String sql) {
        execute(sql);
    }
}
