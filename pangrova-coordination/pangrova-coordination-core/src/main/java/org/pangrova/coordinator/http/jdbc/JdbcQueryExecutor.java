package org.pangrova.coordinator.http.jdbc;

/**
 * Executes SQL queries on behalf of JDBC clients connected via the Avatica wire protocol.
 * Bridges the Avatica Meta layer to Pangrova's internal query lifecycle.
 */
public interface JdbcQueryExecutor {

    /**
     * Executes a SQL statement and returns the result as rows with column metadata.
     *
     * @param sql     the SQL statement to execute
     * @param catalog optional catalog context (nullable)
     * @return query result containing column metadata and row data
     */
    JdbcQueryResult execute(String sql, String catalog);
}
