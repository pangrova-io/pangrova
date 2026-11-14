package org.pangrova.governance.api.exception;

/**
 * Thrown when a governance quality rule SQL query fails to execute.
 *
 * <p>Wraps the underlying cause (parse error, execution timeout, permission denied, etc.)
 * with context about which rule triggered the failure.</p>
 */
public final class GovernanceQueryException extends RuntimeException {

    private final String sql;

    public GovernanceQueryException(String sql, String message) {
        super(message);
        this.sql = sql;
    }

    public GovernanceQueryException(String sql, String message, Throwable cause) {
        super(message, cause);
        this.sql = sql;
    }

    /** Returns the SQL that failed to execute. */
    public String sql() {
        return sql;
    }
}
