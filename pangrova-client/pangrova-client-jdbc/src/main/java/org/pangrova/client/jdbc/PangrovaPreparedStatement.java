package org.pangrova.client.jdbc;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.Date;
import java.sql.NClob;
import java.sql.ParameterMetaData;
import java.sql.PreparedStatement;
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.Map;
import java.util.TreeMap;

/**
 * JDBC PreparedStatement implementation for Pangrova.
 *
 * <p>Binds parameters client-side and inlines them into the SQL text before
 * submitting to the coordinator. This approach avoids requiring server-side
 * prepared statement support while providing BI tool compatibility.</p>
 *
 * <p>Parameter markers ({@code ?}) are replaced positionally with their
 * bound literal values using SQL-safe escaping.</p>
 */
public final class PangrovaPreparedStatement implements PreparedStatement {

    private final PangrovaConnection connection;
    private final String originalSql;
    private final Map<Integer, Object> parameters = new TreeMap<>();
    private boolean closed;
    private int queryTimeoutSeconds;
    private ResultSet currentResultSet;

    PangrovaPreparedStatement(PangrovaConnection connection, String sql) {
        this.connection = connection;
        this.originalSql = sql;
    }

    @Override
    public ResultSet executeQuery() throws SQLException {
        checkOpen();
        closeCurrentResultSet();
        String sql = substituteParameters();
        PangrovaStatement delegate = new PangrovaStatement(connection);
        try {
            ResultSet rs = delegate.executeQuery(sql);
            currentResultSet = rs;
            return rs;
        } catch (SQLException e) {
            delegate.close();
            throw e;
        }
    }

    @Override
    public int executeUpdate() throws SQLException {
        throw new SQLFeatureNotSupportedException("executeUpdate not supported");
    }

    @Override
    public boolean execute() throws SQLException {
        throw new SQLFeatureNotSupportedException("execute not supported");
    }

    @Override
    public void clearParameters() {
        parameters.clear();
    }

    @Override
    public void setNull(int parameterIndex, int sqlType) {
        parameters.put(parameterIndex, null);
    }

    @Override
    public void setBoolean(int parameterIndex, boolean x) {
        parameters.put(parameterIndex, x);
    }

    @Override
    public void setByte(int parameterIndex, byte x) {
        parameters.put(parameterIndex, (int) x);
    }

    @Override
    public void setShort(int parameterIndex, short x) {
        parameters.put(parameterIndex, (int) x);
    }

    @Override
    public void setInt(int parameterIndex, int x) throws SQLException {
        checkOpen();
        parameters.put(parameterIndex, x);
    }

    @Override
    public void setLong(int parameterIndex, long x) {
        parameters.put(parameterIndex, x);
    }

    @Override
    public void setFloat(int parameterIndex, float x) {
        parameters.put(parameterIndex, x);
    }

    @Override
    public void setDouble(int parameterIndex, double x) {
        parameters.put(parameterIndex, x);
    }

    @Override
    public void setBigDecimal(int parameterIndex, BigDecimal x) {
        parameters.put(parameterIndex, x);
    }

    @Override
    public void setString(int parameterIndex, String x) {
        parameters.put(parameterIndex, x);
    }

    @Override
    public void setDate(int parameterIndex, Date x) {
        parameters.put(parameterIndex, x);
    }

    @Override
    public void setTime(int parameterIndex, Time x) {
        parameters.put(parameterIndex, x);
    }

    @Override
    public void setTimestamp(int parameterIndex, Timestamp x) {
        parameters.put(parameterIndex, x);
    }

    @Override
    public void setObject(int parameterIndex, Object x) {
        parameters.put(parameterIndex, x);
    }

    @Override
    public void setObject(int parameterIndex, Object x, int targetSqlType) {
        parameters.put(parameterIndex, x);
    }

    @Override
    public void setObject(int parameterIndex, Object x, int targetSqlType, int scaleOrLength) {
        parameters.put(parameterIndex, x);
    }

    @Override
    public void addBatch() throws SQLException {
        throw new SQLFeatureNotSupportedException("addBatch not supported");
    }

    @Override
    public ResultSetMetaData getMetaData() throws SQLException {
        return currentResultSet != null ? currentResultSet.getMetaData() : null;
    }

    @Override
    public ParameterMetaData getParameterMetaData() throws SQLException {
        checkOpen();
        return new PangrovaParameterMetaData(countPlaceholders(originalSql));
    }

    @Override
    public void close() throws SQLException {
        if (!closed) {
            closed = true;
            closeCurrentResultSet();
        }
    }

    @Override
    public boolean isClosed() {
        return closed;
    }

    @Override
    public void setQueryTimeout(int seconds) {
        this.queryTimeoutSeconds = Math.max(0, seconds);
    }

    @Override
    public int getQueryTimeout() {
        return queryTimeoutSeconds;
    }

    @Override
    public Connection getConnection() {
        return connection;
    }

    // --- Unsupported setters ---

    @Override
    public void setBytes(int parameterIndex, byte[] x) throws SQLException {
        throw new SQLFeatureNotSupportedException("setBytes not supported");
    }

    @Override
    public void setAsciiStream(int parameterIndex, InputStream x, int length) throws SQLException {
        throw new SQLFeatureNotSupportedException("setAsciiStream not supported");
    }

    @Override
    @SuppressWarnings("deprecation")
    public void setUnicodeStream(int parameterIndex, InputStream x, int length) throws SQLException {
        throw new SQLFeatureNotSupportedException("setUnicodeStream not supported");
    }

    @Override
    public void setBinaryStream(int parameterIndex, InputStream x, int length) throws SQLException {
        throw new SQLFeatureNotSupportedException("setBinaryStream not supported");
    }

    @Override
    public void setCharacterStream(int parameterIndex, Reader reader, int length) throws SQLException {
        throw new SQLFeatureNotSupportedException("setCharacterStream not supported");
    }

    @Override
    public void setRef(int parameterIndex, Ref x) throws SQLException {
        throw new SQLFeatureNotSupportedException("setRef not supported");
    }

    @Override
    public void setBlob(int parameterIndex, Blob x) throws SQLException {
        throw new SQLFeatureNotSupportedException("setBlob not supported");
    }

    @Override
    public void setClob(int parameterIndex, Clob x) throws SQLException {
        throw new SQLFeatureNotSupportedException("setClob not supported");
    }

    @Override
    public void setArray(int parameterIndex, Array x) throws SQLException {
        throw new SQLFeatureNotSupportedException("setArray not supported");
    }

    @Override
    public void setDate(int parameterIndex, Date x, Calendar cal) throws SQLException {
        parameters.put(parameterIndex, x);
    }

    @Override
    public void setTime(int parameterIndex, Time x, Calendar cal) throws SQLException {
        parameters.put(parameterIndex, x);
    }

    @Override
    public void setTimestamp(int parameterIndex, Timestamp x, Calendar cal) throws SQLException {
        parameters.put(parameterIndex, x);
    }

    @Override
    public void setNull(int parameterIndex, int sqlType, String typeName) {
        parameters.put(parameterIndex, null);
    }

    @Override
    public void setURL(int parameterIndex, URL x) throws SQLException {
        throw new SQLFeatureNotSupportedException("setURL not supported");
    }

    @Override
    public void setRowId(int parameterIndex, RowId x) throws SQLException {
        throw new SQLFeatureNotSupportedException("setRowId not supported");
    }

    @Override
    public void setNString(int parameterIndex, String value) {
        parameters.put(parameterIndex, value);
    }

    @Override
    public void setNCharacterStream(int parameterIndex, Reader value, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException("setNCharacterStream not supported");
    }

    @Override
    public void setNClob(int parameterIndex, NClob value) throws SQLException {
        throw new SQLFeatureNotSupportedException("setNClob not supported");
    }

    @Override
    public void setClob(int parameterIndex, Reader reader, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException("setClob not supported");
    }

    @Override
    public void setBlob(int parameterIndex, InputStream inputStream, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException("setBlob not supported");
    }

    @Override
    public void setNClob(int parameterIndex, Reader reader, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException("setNClob not supported");
    }

    @Override
    public void setSQLXML(int parameterIndex, SQLXML xmlObject) throws SQLException {
        throw new SQLFeatureNotSupportedException("setSQLXML not supported");
    }

    @Override
    public void setAsciiStream(int parameterIndex, InputStream x, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException("setAsciiStream not supported");
    }

    @Override
    public void setBinaryStream(int parameterIndex, InputStream x, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException("setBinaryStream not supported");
    }

    @Override
    public void setCharacterStream(int parameterIndex, Reader reader, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException("setCharacterStream not supported");
    }

    @Override
    public void setAsciiStream(int parameterIndex, InputStream x) throws SQLException {
        throw new SQLFeatureNotSupportedException("setAsciiStream not supported");
    }

    @Override
    public void setBinaryStream(int parameterIndex, InputStream x) throws SQLException {
        throw new SQLFeatureNotSupportedException("setBinaryStream not supported");
    }

    @Override
    public void setCharacterStream(int parameterIndex, Reader reader) throws SQLException {
        throw new SQLFeatureNotSupportedException("setCharacterStream not supported");
    }

    @Override
    public void setNCharacterStream(int parameterIndex, Reader value) throws SQLException {
        throw new SQLFeatureNotSupportedException("setNCharacterStream not supported");
    }

    @Override
    public void setClob(int parameterIndex, Reader reader) throws SQLException {
        throw new SQLFeatureNotSupportedException("setClob not supported");
    }

    @Override
    public void setBlob(int parameterIndex, InputStream inputStream) throws SQLException {
        throw new SQLFeatureNotSupportedException("setBlob not supported");
    }

    @Override
    public void setNClob(int parameterIndex, Reader reader) throws SQLException {
        throw new SQLFeatureNotSupportedException("setNClob not supported");
    }

    // --- Statement interface methods ---

    @Override public ResultSet executeQuery(String sql) throws SQLException { throw unsupported(); }
    @Override public int executeUpdate(String sql) throws SQLException { throw unsupported(); }
    @Override public boolean execute(String sql) throws SQLException { throw unsupported(); }
    @Override public int getMaxFieldSize() { return 0; }
    @Override public void setMaxFieldSize(int max) {}
    @Override public int getMaxRows() { return 0; }
    @Override public void setMaxRows(int max) {}
    @Override public void setEscapeProcessing(boolean enable) {}
    @Override public void cancel() throws SQLException {}
    @Override public SQLWarning getWarnings() { return null; }
    @Override public void clearWarnings() {}
    @Override public void setCursorName(String name) throws SQLException { throw unsupported(); }
    @Override public ResultSet getResultSet() throws SQLException { return currentResultSet; }
    @Override public int getUpdateCount() { return -1; }
    @Override public boolean getMoreResults() { return false; }
    @Override public void setFetchDirection(int direction) {}
    @Override public int getFetchDirection() { return ResultSet.FETCH_FORWARD; }
    @Override public void setFetchSize(int rows) {}
    @Override public int getFetchSize() { return 0; }
    @Override public int getResultSetConcurrency() { return ResultSet.CONCUR_READ_ONLY; }
    @Override public int getResultSetType() { return ResultSet.TYPE_FORWARD_ONLY; }
    @Override public void addBatch(String sql) throws SQLException { throw unsupported(); }
    @Override public void clearBatch() throws SQLException { throw unsupported(); }
    @Override public int[] executeBatch() throws SQLException { throw unsupported(); }
    @Override public boolean getMoreResults(int current) { return false; }
    @Override public ResultSet getGeneratedKeys() throws SQLException { throw unsupported(); }
    @Override public int executeUpdate(String sql, int autoGeneratedKeys) throws SQLException { throw unsupported(); }
    @Override public int executeUpdate(String sql, int[] columnIndexes) throws SQLException { throw unsupported(); }
    @Override public int executeUpdate(String sql, String[] columnNames) throws SQLException { throw unsupported(); }
    @Override public boolean execute(String sql, int autoGeneratedKeys) throws SQLException { throw unsupported(); }
    @Override public boolean execute(String sql, int[] columnIndexes) throws SQLException { throw unsupported(); }
    @Override public boolean execute(String sql, String[] columnNames) throws SQLException { throw unsupported(); }
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

    /**
     * Substitutes parameter markers with their bound literal values.
     * Returns the final SQL string ready for execution.
     */
    String substituteParameters() throws SQLException {
        return SqlParameterSubstitutor.substitute(originalSql, parameters);
    }

    /**
     * Counts the number of parameter placeholders in a SQL string,
     * ignoring question marks inside single-quoted string literals and double-quoted identifiers.
     */
    static int countPlaceholders(String sql) {
        return SqlParameterSubstitutor.countPlaceholders(sql);
    }

    private void checkOpen() throws SQLException {
        if (closed) {
            throw new SQLException("PreparedStatement is closed");
        }
    }

    private void closeCurrentResultSet() throws SQLException {
        if (currentResultSet != null && !currentResultSet.isClosed()) {
            currentResultSet.close();
        }
        currentResultSet = null;
    }

    private static SQLFeatureNotSupportedException unsupported() {
        return new SQLFeatureNotSupportedException("Operation not supported on PreparedStatement");
    }
}
