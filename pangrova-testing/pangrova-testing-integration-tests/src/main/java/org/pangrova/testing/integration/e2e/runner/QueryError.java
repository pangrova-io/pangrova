package org.pangrova.testing.integration.e2e.runner;

import org.pangrova.testing.integration.e2e.E2ETestFramework;

import java.util.Objects;

/**
 * Structured error from a failed SQL execution.
 * Classifies errors into SYNTAX, SEMANTIC, RUNTIME, or TIMEOUT.
 */
public record QueryError(
    String errorType,
    String message,
    String sqlState
) {

    public QueryError {
        Objects.requireNonNull(errorType, "errorType must not be null");
    }

    /**
     * Build a QueryError from a failed QueryResult.
     * Parses the error message to classify the error type.
     */
    public static QueryError from(E2ETestFramework.QueryResult result) {
        String msg = result.errorMessage() != null ? result.errorMessage() : "Unknown error";
        String type = classifyError(msg);
        String state = extractSqlState(msg);
        return new QueryError(type, msg, state);
    }

    private static String classifyError(String message) {
        String lower = message.toLowerCase();
        if (lower.contains("syntax") || lower.contains("parse")) {
            return "SYNTAX";
        }
        if (lower.contains("not found") || lower.contains("not exist")
                || lower.contains("unknown column") || lower.contains("semantic")
                || lower.contains("ambiguous")) {
            return "SEMANTIC";
        }
        if (lower.contains("timeout") || lower.contains("timed out")) {
            return "TIMEOUT";
        }
        return "RUNTIME";
    }

    private static String extractSqlState(String message) {
        // SQL state codes follow pattern: 5 alphanumeric chars (e.g., "42000")
        if (message == null) {
            return null;
        }
        int idx = message.indexOf("SQLSTATE:");
        if (idx >= 0 && idx + 15 <= message.length()) {
            return message.substring(idx + 9, idx + 14).trim();
        }
        return null;
    }
}
