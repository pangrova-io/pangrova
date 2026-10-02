package org.pangrova.client.jdbc;

import org.pangrova.client.rest.PangrovaClientException;
import org.pangrova.client.rest.PangrovaConnectionException;
import org.pangrova.client.rest.PangrovaProtocolException;
import org.pangrova.client.rest.PangrovaRestClient;
import org.pangrova.client.rest.PangrovaServerException;
import org.pangrova.protocol.rest.query.QueryStatusResponse;
import org.pangrova.protocol.rest.query.SubmitQueryResponse;
import org.pangrova.transport.ArrowBatchDecoder;
import org.pangrova.transport.ResultBatch;
import org.pangrova.transport.TransportException;
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
import java.util.concurrent.atomic.AtomicReference;

/**
 * JDBC Statement that submits queries via {@link PangrovaRestClient},
 * polls for completion, fetches Arrow results, and wraps them in {@link PangrovaResultSet}.
 */
public final class PangrovaStatement implements java.sql.Statement {

    private static final Logger LOG = LoggerFactory.getLogger(PangrovaStatement.class);
    private static final String STATE_FINISHED = "FINISHED";
    /** Length of the WITH keyword, the scan start position for CTE parsing. */
    private static final int WITH_KEYWORD_LENGTH = 4;

    private final PangrovaConnection connection;
    private boolean closed;
    private int queryTimeoutSeconds;
    private int fetchSize;
    private ResultSet currentResultSet;
    private int currentUpdateCount = -1;

    // Thread-safe query ID tracking for cancel() support.
    private final AtomicReference<String> currentQueryId = new AtomicReference<>();

    PangrovaStatement(PangrovaConnection connection) {
        this.connection = connection;
    }

    @Override
    public ResultSet executeQuery(String sql) throws SQLException {
        checkOpen();
        closeCurrentResultSet();
        try {
            return executeQueryInternal(sql);
        } catch (SQLException e) {
            throw e;
        } catch (Exception e) {
            throw toSqlException(e);
        }
    }

    private ResultSet executeQueryInternal(String sql) throws Exception {
        PangrovaRestClient client = connection.restClient();
        Map<String, String> props = buildSessionProperties();
        long deadlineMs = QueryCompletionWaiter.deadlineFor(queryTimeoutSeconds);
        SubmitQueryResponse submitResponse = client.submitQuery(sql, props);
        String queryId = submitResponse.queryId();
        currentQueryId.set(queryId);
        LOG.debug("Submitted query queryId={}", queryId);
        boolean success = false;
        try {
            ResultSet rs = fetchQueryResult(client, queryId, submitResponse, deadlineMs);
            success = true;
            return rs;
        } finally {
            currentQueryId.set(null);
            cancelOnFailure(client, queryId, success);
        }
    }

    private ResultSet fetchQueryResult(PangrovaRestClient client, String queryId,
                                       SubmitQueryResponse submitResponse,
                                       long deadlineMs) throws Exception {
        QueryCompletionWaiter.ReadyEndpoint ready = QueryCompletionWaiter.awaitReadableEndpoint(
                client, queryId, submitResponse, deadlineMs, queryTimeoutSeconds);
        byte[] arrowData;
        try {
            arrowData = client.fetchResult(ready.endpoint(), deadlineMs);
        } catch (PangrovaConnectionException failure) {
            if ("TRANSPORT_QUERY_DEADLINE_EXCEEDED".equals(failure.errorCode())) {
                throw new SQLTimeoutException(
                        "Query timed out after " + queryTimeoutSeconds
                                + "s while reading results, queryId=" + queryId,
                        failure);
            }
            throw failure;
        }
        QueryCompletionWaiter.checkTimeout(
                client, queryId, deadlineMs, queryTimeoutSeconds);
        if (!ready.terminal()) {
            QueryCompletionWaiter.awaitTerminalSuccess(
                    client, queryId, deadlineMs, queryTimeoutSeconds);
        }
        ResultSet rs = createResultSet(arrowData);
        currentResultSet = rs;
        return rs;
    }

    private void cancelOnFailure(PangrovaRestClient client, String queryId, boolean success) {
        if (!success) {
            try {
                client.cancelQuery(queryId);
            } catch (Exception ignored) {
                LOG.debug("event=cancel_on_disconnect_failed queryId={}", queryId);
            }
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
        if (!closed) {
            closed = true;
            closeCurrentResultSet();
        }
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

    private ResultSet createResultSet(byte[] arrowData) throws SQLException {
        try {
            ResultBatch batch = ArrowBatchDecoder.decode(arrowData);
            return new PangrovaResultSet(batch, this);
        } catch (TransportException e) {
            throw new SQLException("Failed to decode result: " + e.getMessage(), e);
        }
    }

    private void checkOpen() throws SQLException {
        if (closed) {
            throw new SQLException("Statement is closed");
        }
    }

    // --- execute / executeUpdate ---

    @Override
    public boolean execute(String sql) throws SQLException {
        checkOpen();
        closeCurrentResultSet();
        currentUpdateCount = -1;
        try {
            PangrovaRestClient client = connection.restClient();
            Map<String, String> props = buildSessionProperties();
            long deadlineMs = QueryCompletionWaiter.deadlineFor(queryTimeoutSeconds);
            SubmitQueryResponse submitResponse = client.submitQuery(sql, props);
            String queryId = submitResponse.queryId();
            currentQueryId.set(queryId);
            boolean success = false;
            try {
                if (isUpdateStatement(sql)) {
                    currentUpdateCount = fetchUpdateCount(
                            client, queryId, deadlineMs);
                    success = true;
                    return false;
                } else {
                    currentResultSet = fetchQueryResult(
                            client, queryId, submitResponse, deadlineMs);
                    success = true;
                    return true;
                }
            } finally {
                currentQueryId.set(null);
                cancelOnFailure(client, queryId, success);
            }
        } catch (SQLException e) {
            throw e;
        } catch (Exception e) {
            throw toSqlException(e);
        }
    }

    @Override
    public int executeUpdate(String sql) throws SQLException {
        checkOpen();
        closeCurrentResultSet();
        currentUpdateCount = -1;
        try {
            PangrovaRestClient client = connection.restClient();
            Map<String, String> props = buildSessionProperties();
            long deadlineMs = QueryCompletionWaiter.deadlineFor(queryTimeoutSeconds);
            SubmitQueryResponse submitResponse = client.submitQuery(sql, props);
            String queryId = submitResponse.queryId();
            currentQueryId.set(queryId);
            boolean success = false;
            try {
                int count = fetchUpdateCount(client, queryId, deadlineMs);
                currentUpdateCount = count;
                success = true;
                return count;
            } finally {
                currentQueryId.set(null);
                cancelOnFailure(client, queryId, success);
            }
        } catch (SQLException e) {
            throw e;
        } catch (Exception e) {
            throw toSqlException(e);
        }
    }

    private int fetchUpdateCount(
            PangrovaRestClient client, String queryId, long deadlineMs)
            throws SQLException {
        QueryStatusResponse status = QueryCompletionWaiter.awaitTerminalSuccess(
                client, queryId, deadlineMs, queryTimeoutSeconds);
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
                Boolean outcome = classifyTopLevelKeyword(sql, i);
                if (outcome != null) {
                    return outcome;
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
     * @return TRUE for a DML verb, FALSE for SELECT, null for anything else
     */
    private static Boolean classifyTopLevelKeyword(String sql, int start) {
        int len = sql.length();
        int wordEnd = start;
        while (wordEnd < len && !Character.isWhitespace(sql.charAt(wordEnd))
                && sql.charAt(wordEnd) != '(' && sql.charAt(wordEnd) != ',') {
            wordEnd++;
        }
        String word = sql.substring(start, wordEnd).toUpperCase(Locale.ROOT);
        if (isDmlKeyword(word)) {
            return Boolean.TRUE;
        }
        if (isSelectKeyword(word)) {
            return Boolean.FALSE;
        }
        return null;
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
        // Atomic getAndSet ensures the cancel thread gets the queryId exactly once,
        // even if executeQueryInternal's finally block races to clear it.
        String qid = currentQueryId.getAndSet(null);
        if (qid != null) {
            try {
                connection.restClient().cancelQuery(qid);
            } catch (Exception e) {
                throw new SQLException("Failed to cancel query queryId=" + qid, e);
            }
        }
    }
    @Override public SQLWarning getWarnings() { return null; }
    @Override public void clearWarnings() {}
    @Override public void setCursorName(String name) throws SQLException { throw unsupported(); }
    @Override public ResultSet getResultSet() { return currentResultSet; }
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
    @Override public Connection getConnection() { return connection; }
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
