package org.pangrova.testing.e2e.runner;

import org.pangrova.testing.e2e.E2ETestFramework;

/**
 * Thrown when a SQL execution fails unexpectedly.
 * Carries the original SQL text and error details for diagnostics.
 */
public final class SqlExecutionException extends RuntimeException {

    private final String sql;
    private final String errorMessage;

    public SqlExecutionException(String sql, E2ETestFramework.QueryResult result) {
        super(buildMessage(sql, result));
        this.sql = sql;
        this.errorMessage = result.errorMessage();
    }

    public String sql() {
        return sql;
    }

    public String errorMessage() {
        return errorMessage;
    }

    private static String buildMessage(String sql, E2ETestFramework.QueryResult result) {
        String error = result.errorMessage() != null ? result.errorMessage() : "Unknown error";
        return "SQL execution failed [" + error + "] for: " + sql;
    }
}
