package org.pangrova.testing.integration.exception;

/**
 * Thrown when a SQL execution fails unexpectedly.
 * Carries the original SQL text and error details for diagnostics.
 */
public final class SqlExecutionException extends RuntimeException {

    private final String sql;
    private final String errorMessage;

    public SqlExecutionException(String sql, String errorMessage) {
        super(buildMessage(sql, errorMessage));
        this.sql = sql;
        this.errorMessage = errorMessage;
    }

    public String sql() {
        return sql;
    }

    public String errorMessage() {
        return errorMessage;
    }

    private static String buildMessage(String sql, String errorMessage) {
        String error = errorMessage != null ? errorMessage : "Unknown error";
        return "SQL execution failed [" + error + "] for: " + sql;
    }
}
