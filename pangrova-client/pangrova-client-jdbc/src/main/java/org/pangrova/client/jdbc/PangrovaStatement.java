package org.pangrova.client.jdbc;

import org.pangrova.client.jdbc.enums.StatementKeywordKind;

import org.pangrova.common.client.enums.CancelOutcome;

import org.pangrova.common.client.exception.PangrovaClientException;
import org.pangrova.common.client.exception.PangrovaConnectionException;
import org.pangrova.common.client.exception.PangrovaProtocolException;
import org.pangrova.client.http.PangrovaRestClient;
import org.pangrova.client.http.QueryExecution;
import org.pangrova.common.client.exception.PangrovaServerException;
import org.pangrova.common.client.query.QueryStatusResponse;
import org.pangrova.transport.client.ResultCursor;
import org.pangrova.transport.client.exception.TransportConnectionException;
import org.pangrova.transport.client.exception.TransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLTimeoutException;
import java.sql.SQLWarning;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicReference;

/**
 * JDBC Statement that submits queries via {@link PangrovaRestClient},
 * polls for completion, fetches Arrow results, and wraps them in {@link PangrovaResultSet}.
 */
public final class PangrovaStatement implements java.sql.Statement {

    private static final Logger LOG = LoggerFactory.getLogger(PangrovaStatement.class);
    /** Length of the WITH keyword, the scan start position for CTE parsing. */
    private static final int WITH_KEYWORD_LENGTH = 4;

    private final PangrovaConnection connection;
    private volatile boolean closed;
    // The future belongs to one invocation, including the interval before submit returns its ID.
    private final AtomicReference<CompletableFuture<QueryExecution>> submission = new AtomicReference<>();
    private int queryTimeoutSeconds;
    private int fetchSize;
    private ResultSet currentResultSet;
    private int currentUpdateCount = -1;

    private final AtomicReference<QueryExecution> currentQuery = new AtomicReference<>();

    PangrovaStatement(PangrovaConnection connection) {
        this.connection = connection;
    }

    @Override
    public ResultSet executeQuery(String sql) throws SQLException {
        return executeQuerySql(sql, this);
    }

    /** Bound and plain JDBC queries share the same submission, deadline and cancellation owner. */
    ResultSet executeBoundQuery(String sql, Map<Integer, Object> parameters, java.sql.Statement producer)
            throws SQLException {
        checkOpen();
        return executeQuerySql(SqlParameterSubstitutor.substitute(sql, parameters), producer);
    }

    private ResultSet executeQuerySql(String sql, java.sql.Statement producer) throws SQLException {
        checkOpen();
        acquireExecution();
        try {
            closeCurrentResultSet();
            return runQuery(sql, (client, query, deadlineMs) -> fetchQueryResult(client, query, deadlineMs, producer));
        } catch (SQLException e) {
            throw e;
        } catch (Exception e) {
            throw toSqlException(e);
        } finally {
            releaseExecution();
        }
    }

    private <T> T runQuery(String sql, QueryOperation<T> operation) throws Exception {
        if (currentQuery.get() != null) {
            throw new SQLException("Previous query cancellation is unresolved; call cancel before executing", "HY010");
        }
        PangrovaRestClient client = connection.restClient();
        Map<String, String> props = buildSessionProperties();
        long deadlineMs = QueryCompletionWaiter.deadlineFor(queryTimeoutSeconds);
        QueryExecution query = client.beginQuery(sql, props);
        currentQuery.set(query);
        submission.get().complete(query);
        LOG.debug("Submitted query queryId={}", query.submission().queryId());
        try {
            T result = operation.execute(client, query, deadlineMs);
            if (currentResultSet == null) currentQuery.compareAndSet(query, null);
            return result;
        } catch (Exception failure) {
            cancelOnFailure(query, failure);
            throw failure;
        }
    }

    private void acquireExecution() throws SQLException {
        if (!submission.compareAndSet(null, new CompletableFuture<>())) {
            throw new SQLException("Statement execution is still owned", "HY010");
        }
        try {
            checkOpen();
        } catch (SQLException failure) {
            releaseExecution();
            throw failure;
        }
    }

    private void releaseExecution() {
        var completed = submission.getAndSet(null);
        // Also unblock a cancel waiting on an unsuccessful or unconfirmed submission.
        completed.complete(null);
    }

    @FunctionalInterface
    private interface QueryOperation<T> {
        T execute(PangrovaRestClient client, QueryExecution query, long deadlineMs)
                throws Exception;
    }

    private ResultSet fetchQueryResult(PangrovaRestClient client, QueryExecution query,
                                       long deadlineMs, java.sql.Statement producer) throws Exception {
        String queryId = query.submission().queryId();
        QueryCompletionWaiter.ReadyEndpoint ready =
                QueryCompletionWaiter.awaitReadableEndpoint(query, deadlineMs, queryTimeoutSeconds);
        int timeoutSeconds = queryTimeoutSeconds;
        ResultCursor cursor;
        try {
            cursor = client.openResult(ready.endpoint(), deadlineMs);
        } catch (TransportException failure) {
            throw resultFailure(failure, queryId, timeoutSeconds);
        }
        try {
            checkOpen();
            var result = new PangrovaResultSet(cursor, producer, new PangrovaResultSet.Completion() {
                @Override
                public void finish() throws SQLException {
                    QueryCompletionWaiter.checkTimeout(queryId, deadlineMs, timeoutSeconds);
                    if (!ready.terminal()) {
                        try {
                            QueryCompletionWaiter.awaitTerminalSuccess(query, deadlineMs, timeoutSeconds);
                        } catch (PangrovaClientException failure) {
                            throw toSqlException(failure);
                        }
                    }
                    currentQuery.compareAndSet(query, null);
                }

                @Override
                public void abandon() throws SQLException {
                    cancelOwnedQuery(query);
                }

                @Override
                public SQLException translate(TransportException failure) {
                    return resultFailure(failure, queryId, timeoutSeconds);
                }
            });
            currentResultSet = result;
            if (ready.terminal()) currentQuery.compareAndSet(query, null);
            return result;
        } catch (SQLException | RuntimeException failure) {
            try {
                cursor.close();
            } catch (RuntimeException cleanup) {
                failure.addSuppressed(cleanup);
            }
            throw failure;
        }
    }

    private static SQLException resultFailure(TransportException failure, String queryId, int timeoutSeconds) {
        String code = failure.transportErrorCode();
        if ("TRANSPORT_QUERY_DEADLINE_EXCEEDED".equals(code)) {
            return new SQLTimeoutException("Query timed out after " + timeoutSeconds
                    + "s while reading results, queryId=" + queryId, failure);
        }
        return new SQLException("[" + code + "] " + failure.getMessage(),
                failure instanceof TransportConnectionException ? "08001" : "HY000", failure);
    }

    private void cancelOnFailure(QueryExecution query, Exception failure) {
        try {
            cancelOwnedQuery(query);
        } catch (SQLException cleanup) {
            // Keep the exact query identity for explicit retry and preserve the primary error.
            failure.addSuppressed(cleanup);
            LOG.debug("event=cancel_on_disconnect_failed queryId={}", query.submission().queryId(), cleanup);
        }
    }

    private static SQLException toSqlException(Exception e) {
        if (e instanceof PangrovaConnectionException ace) {
            return new SQLException("[" + ace.errorCode() + "] " + ace.getMessage(), "08001", ace);
        }
        if (e instanceof PangrovaProtocolException ape) {
            return new SQLException("[" + ape.errorCode() + "] " + ape.getMessage(), "08P01", ape);
        }
        if (e instanceof PangrovaServerException ase) {
            return new SQLException("[" + ase.errorCode() + "] " + ase.getMessage(), "HY000", ase);
        }
        if (e instanceof PangrovaClientException acl) {
            return new SQLException("[" + acl.errorCode() + "] " + acl.getMessage(), acl);
        }
        return new SQLException("Query execution failed: " + e.getMessage(), e);
    }

    @Override
    public void close() throws SQLException {
        closed = true;
        if (submission.get() != null) {
            throw new SQLException("Statement execution has not drained; cancel and retry close", "HY010");
        }
        if (currentQuery.get() != null) {
            cancel();
        }
        closeCurrentResultSet();
        connection.statementClosed(this);
    }

    private void closeCurrentResultSet() throws SQLException {
        if (currentResultSet != null && !currentResultSet.isClosed()) {
            currentResultSet.close();
        }
        currentResultSet = null;
    }

    @Override
    public boolean isClosed() {
        return closed;
    }

    // --- Internal helpers ---

    private Map<String, String> buildSessionProperties() {
        String catalog = connection.catalogName();
        if (catalog != null && !catalog.isEmpty()) {
            return Map.of("catalog", catalog);
        }
        return Map.of();
    }

    private void checkOpen() throws SQLException {
        if (closed || connection.isClosed()) {
            throw new SQLException("Statement is closed");
        }
    }

    // --- execute / executeUpdate ---

    @Override
    public boolean execute(String sql) throws SQLException {
        checkOpen();
        acquireExecution();
        try {
            closeCurrentResultSet();
            currentUpdateCount = -1;
            return runQuery(sql, (client, query, deadlineMs) -> {
                if (isUpdateStatement(sql)) {
                    currentUpdateCount = fetchUpdateCount(query, deadlineMs);
                    return false;
                } else {
                    currentResultSet = fetchQueryResult(client, query, deadlineMs, this);
                    return true;
                }
            });
        } catch (SQLException e) {
            throw e;
        } catch (Exception e) {
            throw toSqlException(e);
        } finally {
            releaseExecution();
        }
    }

    @Override
    public int executeUpdate(String sql) throws SQLException {
        checkOpen();
        acquireExecution();
        try {
            closeCurrentResultSet();
            currentUpdateCount = -1;
            return runQuery(sql, (client, query, deadlineMs) -> {
                int count = fetchUpdateCount(query, deadlineMs);
                currentUpdateCount = count;
                return count;
            });
        } catch (SQLException e) {
            throw e;
        } catch (Exception e) {
            throw toSqlException(e);
        } finally {
            releaseExecution();
        }
    }

    private int fetchUpdateCount(QueryExecution query, long deadlineMs) throws SQLException {
        QueryStatusResponse status = QueryCompletionWaiter.awaitTerminalSuccess(query, deadlineMs, queryTimeoutSeconds);
        String queryId = query.submission().queryId();
        if (status.stats() != null) {
            long affectedRows = status.stats().rowsReturned();
            if (affectedRows < 0 || affectedRows > Integer.MAX_VALUE) {
                throw new SQLException(
                        "Affected row count exceeds JDBC int range, queryId=" + queryId);
            }
            return (int) affectedRows;
        }
        return 0;
    }

    static boolean isUpdateStatement(String sql) {
        String stripped = stripLeadingComments(sql).stripLeading();
        if (stripped.isEmpty()) {
            return false;
        }
        int end = 0;
        while (end < stripped.length() && !Character.isWhitespace(stripped.charAt(end))) {
            end++;
        }
        String keyword = stripped.substring(0, end).toUpperCase(Locale.ROOT);
        if ("WITH".equals(keyword)) {
            // CTE-wrapped DML: skip past balanced parentheses to find the terminal keyword
            return isCteWrappedDml(stripped);
        }
        return switch (keyword) {
            case "INSERT", "UPDATE", "DELETE", "MERGE",
                 "CREATE", "DROP", "ALTER", "TRUNCATE",
                 "COMPACT", "VACUUM", "GRANT", "REVOKE" -> true;
            default -> false;
        };
    }

    /**
     * Determines if a WITH statement wraps a DML operation by finding the
     * terminal keyword after the CTE definitions.
     */
    private static boolean isCteWrappedDml(String sql) {
        // Skip past "WITH" keyword
        int i = WITH_KEYWORD_LENGTH;
        int len = sql.length();
        int parenDepth = 0;
        boolean inSingleQuote = false;

        while (i < len) {
            char c = sql.charAt(i);
            if (c == '\'') {
                inSingleQuote = !inSingleQuote;
            } else if (isParenOutsideQuotes(c, inSingleQuote)) {
                parenDepth += parenDelta(c);
            } else if (isTopLevelTokenStart(c, parenDepth, inSingleQuote)) {
                // At top-level, check if we've found a keyword (non-CTE-name position)
                // Look for the terminal keyword after the last CTE closing paren
                // by finding a keyword that's a DML verb
                StatementKeywordKind outcome = classifyTopLevelKeyword(sql, i);
                if (outcome != StatementKeywordKind.OTHER) {
                    return outcome == StatementKeywordKind.DML;
                }
            }
            i++;
        }
        return false;
    }

    private static boolean isParenOutsideQuotes(char c, boolean inSingleQuote) {
        return !inSingleQuote && (c == '(' || c == ')');
    }

    private static int parenDelta(char c) {
        return c == '(' ? 1 : -1;
    }

    private static boolean isTopLevelTokenStart(char c, int parenDepth, boolean inSingleQuote) {
        return !inSingleQuote && parenDepth == 0 && !Character.isWhitespace(c);
    }

    /**
     * Reads the top-level word starting at {@code start} and classifies it.
     *
     * @return an explicit classification; an unknown token is never a nullable Boolean
     */
    private static StatementKeywordKind classifyTopLevelKeyword(String sql, int start) {
        int len = sql.length();
        int wordEnd = start;
        while (wordEnd < len && !Character.isWhitespace(sql.charAt(wordEnd))
                && sql.charAt(wordEnd) != '(' && sql.charAt(wordEnd) != ',') {
            wordEnd++;
        }
        String word = sql.substring(start, wordEnd).toUpperCase(Locale.ROOT);
        if (isDmlKeyword(word)) {
            return StatementKeywordKind.DML;
        }
        if (isSelectKeyword(word)) {
            return StatementKeywordKind.SELECT;
        }
        return StatementKeywordKind.OTHER;
    }


    private static boolean isDmlKeyword(String word) {
        return switch (word) {
            case "INSERT", "UPDATE", "DELETE", "MERGE" -> true;
            default -> false;
        };
    }

    private static boolean isSelectKeyword(String word) {
        return "SELECT".equals(word);
    }

    private static String stripLeadingComments(String sql) {
        String s = sql;
        while (true) {
            s = s.stripLeading();
            if (s.startsWith("--")) {
                int nl = s.indexOf('\n');
                if (nl < 0) {
                    return "";
                }
                s = s.substring(nl + 1);
            } else if (s.startsWith("/*")) {
                int end = s.indexOf("*/");
                if (end < 0) {
                    return "";
                }
                s = s.substring(end + 2);
            } else {
                return s;
            }
        }
    }

    // --- Unsupported operations ---
    @Override public int getMaxFieldSize() { return 0; }
    @Override public void setMaxFieldSize(int max) {}
    @Override public int getMaxRows() { return 0; }
    @Override public void setMaxRows(int max) {}
    @Override public void setEscapeProcessing(boolean enable) {}
    @Override public int getQueryTimeout() { return queryTimeoutSeconds; }
    @Override public void setQueryTimeout(int seconds) {
        this.queryTimeoutSeconds = Math.max(0, seconds);
    }
    @Override public void cancel() throws SQLException {
        var pending = submission.get();
        QueryExecution query;
        if (pending == null) {
            query = currentQuery.get();
        } else {
            try {
                query = pending.get();
                if (query == null) throw new SQLException("Query submission did not return a confirmed identity");
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                throw new SQLException("Interrupted waiting for the query identity to cancel", interrupted);
            } catch (ExecutionException failure) {
                throw new SQLException("Could not resolve the query identity to cancel", failure.getCause());
            }
        }
        if (query != null) cancelOwnedQuery(query);
    }

    private void cancelOwnedQuery(QueryExecution query) throws SQLException {
        if (query != currentQuery.get()) return;
        String queryId = query.submission().queryId();
        boolean interrupted = Thread.interrupted();
        try {
            CancelOutcome outcome = query.cancel();
            if (outcome == CancelOutcome.NOT_FOUND) {
                throw new SQLException("Coordinator did not find active query queryId=" + queryId);
            }
            // The cancellation receipt retires the exact logical owner. Physical drain has its own evidence.
            currentQuery.compareAndSet(query, null);
        } catch (SQLException failure) {
            throw failure;
        } catch (Exception failure) {
            throw new SQLException("Failed to cancel query queryId=" + queryId, failure);
        } finally {
            if (interrupted) Thread.currentThread().interrupt();
        }
    }
    @Override public SQLWarning getWarnings() { return null; }
    @Override public void clearWarnings() {}
    @Override public void setCursorName(String name) throws SQLException { throw unsupported(); }
    @Override public ResultSet getResultSet() throws SQLException {
        checkOpen();
        return currentResultSet;
    }
    @Override public int getUpdateCount() { return currentUpdateCount; }
    @Override
    public boolean getMoreResults() {
        try {
            closeCurrentResultSet();
        } catch (SQLException ignored) {
            // Already-closed result sets are not an error for getMoreResults().
        }
        currentUpdateCount = -1;
        return false;
    }
    @Override public void setFetchDirection(int direction) throws SQLException { throw unsupported(); }
    @Override public int getFetchDirection() { return ResultSet.FETCH_FORWARD; }
    @Override public void setFetchSize(int rows) { this.fetchSize = Math.max(0, rows); }
    @Override public int getFetchSize() { return fetchSize; }
    @Override public int getResultSetConcurrency() { return ResultSet.CONCUR_READ_ONLY; }
    @Override public int getResultSetType() { return ResultSet.TYPE_FORWARD_ONLY; }
    @Override public void addBatch(String sql) throws SQLException { throw unsupported(); }
    @Override public void clearBatch() throws SQLException { throw unsupported(); }
    @Override public int[] executeBatch() throws SQLException { throw unsupported(); }
    @Override public Connection getConnection() throws SQLException {
        checkOpen();
        return connection;
    }
    @Override public boolean getMoreResults(int current) { return getMoreResults(); }
    @Override public ResultSet getGeneratedKeys() throws SQLException { throw unsupported(); }
    @Override public int executeUpdate(String sql, int autoGeneratedKeys) throws SQLException { return executeUpdate(sql); }
    @Override public int executeUpdate(String sql, int[] columnIndexes) throws SQLException { return executeUpdate(sql); }
    @Override public int executeUpdate(String sql, String[] columnNames) throws SQLException { return executeUpdate(sql); }
    @Override public boolean execute(String sql, int autoGeneratedKeys) throws SQLException { return execute(sql); }
    @Override public boolean execute(String sql, int[] columnIndexes) throws SQLException { return execute(sql); }
    @Override public boolean execute(String sql, String[] columnNames) throws SQLException { return execute(sql); }
    @Override public int getResultSetHoldability() { return ResultSet.CLOSE_CURSORS_AT_COMMIT; }
    @Override public boolean isPoolable() { return false; }
    @Override public void setPoolable(boolean poolable) {}
    @Override public void closeOnCompletion() {}
    @Override public boolean isCloseOnCompletion() { return false; }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        if (iface.isInstance(this)) {
            return iface.cast(this);
        }
        throw new SQLException("Cannot unwrap to " + iface.getName());
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) {
        return iface.isInstance(this);
    }

    private static SQLFeatureNotSupportedException unsupported() {
        return new SQLFeatureNotSupportedException("Operation not supported by PangrovaStatement");
    }
}
