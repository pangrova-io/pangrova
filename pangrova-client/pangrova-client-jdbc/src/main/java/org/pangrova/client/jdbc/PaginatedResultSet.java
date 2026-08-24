package org.pangrova.client.jdbc;

import org.pangrova.client.rest.PangrovaRestClient;
import org.pangrova.protocol.rest.query.ResultPage;
import org.pangrova.transport.ArrowBatchDecoder;
import org.pangrova.transport.ColumnDescriptor;
import org.pangrova.transport.ResultBatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.NClob;
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.List;
import java.util.Map;

/**
 * JDBC ResultSet that fetches results page-by-page using cursor-based pagination.
 * Pages are fetched lazily via {@link PangrovaRestClient#fetchResultPage(String, int, String)}.
 */
final class PaginatedResultSet implements ResultSet {

    private static final Logger LOG = LoggerFactory.getLogger(PaginatedResultSet.class);

    private final PangrovaRestClient client;
    private final String queryId;
    private int fetchSize;
    private final Statement owningStatement;

    private List<Object[]> currentRows = List.of();
    private List<ColumnDescriptor> currentColumns;
    private PangrovaResultSetMetaData metaData;
    private int currentRowIndex = -1;
    private long globalRowOffset;
    private String nextToken;
    private boolean closed;
    private boolean wasNull;
    private boolean noMorePages;

    PaginatedResultSet(PangrovaRestClient client, String queryId, int fetchSize,
                       Statement owningStatement) {
        this.client = client;
        this.queryId = queryId;
        this.fetchSize = fetchSize;
        this.owningStatement = owningStatement;
    }

    @Override
    public boolean next() throws SQLException {
        checkOpen();
        currentRowIndex++;
        if (currentRowIndex < currentRows.size()) {
            return true;
        }
        if (noMorePages) {
            return false;
        }
        return fetchNextPage();
    }

    private boolean fetchNextPage() throws SQLException {
        int effectiveFetchSize = fetchSize > 0 ? fetchSize : 1000;
        try {
            ResultPage page = client.fetchResultPage(queryId, effectiveFetchSize, nextToken);
            nextToken = page.nextToken();
            LOG.debug("Fetched page for queryId={}, rowCount={}, hasMore={}",
                    queryId, page.rowCount(), page.hasMore());
            if (!page.hasMore()) {
                noMorePages = true;
            }
            ResultBatch batch = ArrowBatchDecoder.decode(page.data());
            if (currentColumns == null) {
                currentColumns = batch.columns();
                metaData = new PangrovaResultSetMetaData(currentColumns);
            }
            globalRowOffset += currentRows.size();
            currentRows = batch.rows();
            currentRowIndex = 0;
            return !currentRows.isEmpty();
        } catch (Exception e) {
            throw new SQLException("Failed to fetch result page for queryId=" + queryId, e);
        }
    }

    @Override
    public void close() {
        closed = true;
    }

    @Override
    public boolean isClosed() {
        return closed;
    }

    @Override
    public ResultSetMetaData getMetaData() throws SQLException {
        checkOpen();
        if (metaData == null) {
            // JDBC spec: getMetaData() must be callable before next().
            // Eagerly fetch first page to populate column metadata.
            fetchNextPage();
            // Reset cursor to before-first so the first next() still returns row 0.
            currentRowIndex = -1;
        }
        return metaData;
    }

    @Override
    public boolean wasNull() {
        return wasNull;
    }

    @Override
    public String getString(int columnIndex) throws SQLException {
        Object value = getObjectInternal(columnIndex);
        return value == null ? null : value.toString();
    }

    @Override
    public boolean getBoolean(int columnIndex) throws SQLException {
        Object value = getObjectInternal(columnIndex);
        if (value == null) return false;
        if (value instanceof Boolean b) return b;
        return Boolean.parseBoolean(value.toString());
    }

    @Override
    public int getInt(int columnIndex) throws SQLException {
        Object value = getObjectInternal(columnIndex);
        if (value == null) return 0;
        if (value instanceof Number n) return n.intValue();
        return Integer.parseInt(value.toString());
    }

    @Override
    public long getLong(int columnIndex) throws SQLException {
        Object value = getObjectInternal(columnIndex);
        if (value == null) return 0L;
        if (value instanceof Number n) return n.longValue();
        return Long.parseLong(value.toString());
    }

    @Override
    public double getDouble(int columnIndex) throws SQLException {
        Object value = getObjectInternal(columnIndex);
        if (value == null) return 0.0;
        if (value instanceof Number n) return n.doubleValue();
        return Double.parseDouble(value.toString());
    }

    @Override
    public Object getObject(int columnIndex) throws SQLException {
        return getObjectInternal(columnIndex);
    }

    @Override public String getString(String columnLabel) throws SQLException { return getString(findColumn(columnLabel)); }
    @Override public boolean getBoolean(String columnLabel) throws SQLException { return getBoolean(findColumn(columnLabel)); }
    @Override public int getInt(String columnLabel) throws SQLException { return getInt(findColumn(columnLabel)); }
    @Override public long getLong(String columnLabel) throws SQLException { return getLong(findColumn(columnLabel)); }
    @Override public double getDouble(String columnLabel) throws SQLException { return getDouble(findColumn(columnLabel)); }
    @Override public Object getObject(String columnLabel) throws SQLException { return getObject(findColumn(columnLabel)); }

    @Override
    public int findColumn(String columnLabel) throws SQLException {
        checkOpen();
        if (currentColumns == null) {
            throw new SQLException("No data loaded yet");
        }
        for (int i = 0; i < currentColumns.size(); i++) {
            if (currentColumns.get(i).name().equalsIgnoreCase(columnLabel)) {
                return i + 1;
            }
        }
        throw new SQLException("Column not found: " + columnLabel);
    }

    private Object getObjectInternal(int columnIndex) throws SQLException {
        checkOpen();
        checkRow();
        Object[] row = currentRows.get(currentRowIndex);
        if (columnIndex < 1 || columnIndex > row.length) {
            throw new SQLException("Column index out of range: " + columnIndex);
        }
        Object value = row[columnIndex - 1];
        wasNull = value == null;
        return value;
    }

    private void checkOpen() throws SQLException {
        if (closed) throw new SQLException("ResultSet is closed");
    }

    private void checkRow() throws SQLException {
        if (currentRowIndex < 0 || currentRowIndex >= currentRows.size()) {
            throw new SQLException("Invalid cursor position");
        }
    }

    @Override
    public void setFetchSize(int rows) {
        this.fetchSize = Math.max(rows, 0);
    }

    @Override
    public int getFetchSize() {
        return fetchSize;
    }

    // --- Unsupported operations ---

    @Override public byte getByte(int columnIndex) throws SQLException { throw unsupported(); }
    @Override public short getShort(int columnIndex) throws SQLException { throw unsupported(); }
    @Override public float getFloat(int columnIndex) throws SQLException { throw unsupported(); }
    @Override public BigDecimal getBigDecimal(int columnIndex, int scale) throws SQLException { throw unsupported(); }
    @Override public byte[] getBytes(int columnIndex) throws SQLException { throw unsupported(); }
    @Override public Date getDate(int columnIndex) throws SQLException { throw unsupported(); }
    @Override public Time getTime(int columnIndex) throws SQLException { throw unsupported(); }
    @Override public Timestamp getTimestamp(int columnIndex) throws SQLException { throw unsupported(); }
    @Override public InputStream getAsciiStream(int columnIndex) throws SQLException { throw unsupported(); }
    @Override public InputStream getUnicodeStream(int columnIndex) throws SQLException { throw unsupported(); }
    @Override public InputStream getBinaryStream(int columnIndex) throws SQLException { throw unsupported(); }
    @Override public byte getByte(String columnLabel) throws SQLException { throw unsupported(); }
    @Override public short getShort(String columnLabel) throws SQLException { throw unsupported(); }
    @Override public float getFloat(String columnLabel) throws SQLException { throw unsupported(); }
    @Override public BigDecimal getBigDecimal(String columnLabel, int scale) throws SQLException { throw unsupported(); }
    @Override public byte[] getBytes(String columnLabel) throws SQLException { throw unsupported(); }
    @Override public Date getDate(String columnLabel) throws SQLException { throw unsupported(); }
    @Override public Time getTime(String columnLabel) throws SQLException { throw unsupported(); }
    @Override public Timestamp getTimestamp(String columnLabel) throws SQLException { throw unsupported(); }
    @Override public InputStream getAsciiStream(String columnLabel) throws SQLException { throw unsupported(); }
    @Override public InputStream getUnicodeStream(String columnLabel) throws SQLException { throw unsupported(); }
    @Override public InputStream getBinaryStream(String columnLabel) throws SQLException { throw unsupported(); }
    @Override public SQLWarning getWarnings() { return null; }
    @Override public void clearWarnings() {}
    @Override public String getCursorName() throws SQLException { throw unsupported(); }
    @Override public Reader getCharacterStream(int columnIndex) throws SQLException { throw unsupported(); }
    @Override public Reader getCharacterStream(String columnLabel) throws SQLException { throw unsupported(); }
    @Override public BigDecimal getBigDecimal(int columnIndex) throws SQLException { throw unsupported(); }
    @Override public BigDecimal getBigDecimal(String columnLabel) throws SQLException { throw unsupported(); }
    @Override public boolean isBeforeFirst() { return globalRowOffset == 0 && currentRowIndex < 0; }
    @Override public boolean isAfterLast() { return noMorePages && currentRowIndex >= currentRows.size(); }
    @Override public boolean isFirst() { return globalRowOffset == 0 && currentRowIndex == 0; }
    @Override public boolean isLast() { return noMorePages && currentRowIndex == currentRows.size() - 1; }
    @Override public void beforeFirst() throws SQLException { throw unsupported(); }
    @Override public void afterLast() throws SQLException { throw unsupported(); }
    @Override public boolean first() throws SQLException { throw unsupported(); }
    @Override public boolean last() throws SQLException { throw unsupported(); }
    @Override public int getRow() { return currentRowIndex >= 0 ? (int) (globalRowOffset + currentRowIndex + 1) : 0; }
    @Override public boolean absolute(int row) throws SQLException { throw unsupported(); }
    @Override public boolean relative(int rows) throws SQLException { throw unsupported(); }
    @Override public boolean previous() throws SQLException { throw unsupported(); }
    @Override public void setFetchDirection(int direction) throws SQLException { throw unsupported(); }
    @Override public int getFetchDirection() { return ResultSet.FETCH_FORWARD; }
    @Override public int getType() { return ResultSet.TYPE_FORWARD_ONLY; }
    @Override public int getConcurrency() { return ResultSet.CONCUR_READ_ONLY; }
    @Override public boolean rowUpdated() { return false; }
    @Override public boolean rowInserted() { return false; }
    @Override public boolean rowDeleted() { return false; }
    @Override public void updateNull(int columnIndex) throws SQLException { throw unsupported(); }
    @Override public void updateBoolean(int columnIndex, boolean x) throws SQLException { throw unsupported(); }
    @Override public void updateByte(int columnIndex, byte x) throws SQLException { throw unsupported(); }
    @Override public void updateShort(int columnIndex, short x) throws SQLException { throw unsupported(); }
    @Override public void updateInt(int columnIndex, int x) throws SQLException { throw unsupported(); }
    @Override public void updateLong(int columnIndex, long x) throws SQLException { throw unsupported(); }
    @Override public void updateFloat(int columnIndex, float x) throws SQLException { throw unsupported(); }
    @Override public void updateDouble(int columnIndex, double x) throws SQLException { throw unsupported(); }
    @Override public void updateBigDecimal(int columnIndex, BigDecimal x) throws SQLException { throw unsupported(); }
    @Override public void updateString(int columnIndex, String x) throws SQLException { throw unsupported(); }
    @Override public void updateBytes(int columnIndex, byte[] x) throws SQLException { throw unsupported(); }
    @Override public void updateDate(int columnIndex, Date x) throws SQLException { throw unsupported(); }
    @Override public void updateTime(int columnIndex, Time x) throws SQLException { throw unsupported(); }
    @Override public void updateTimestamp(int columnIndex, Timestamp x) throws SQLException { throw unsupported(); }
    @Override public void updateAsciiStream(int columnIndex, InputStream x, int length) throws SQLException { throw unsupported(); }
    @Override public void updateBinaryStream(int columnIndex, InputStream x, int length) throws SQLException { throw unsupported(); }
    @Override public void updateCharacterStream(int columnIndex, Reader x, int length) throws SQLException { throw unsupported(); }
    @Override public void updateObject(int columnIndex, Object x, int scaleOrLength) throws SQLException { throw unsupported(); }
    @Override public void updateObject(int columnIndex, Object x) throws SQLException { throw unsupported(); }
    @Override public void updateNull(String columnLabel) throws SQLException { throw unsupported(); }
    @Override public void updateBoolean(String columnLabel, boolean x) throws SQLException { throw unsupported(); }
    @Override public void updateByte(String columnLabel, byte x) throws SQLException { throw unsupported(); }
    @Override public void updateShort(String columnLabel, short x) throws SQLException { throw unsupported(); }
    @Override public void updateInt(String columnLabel, int x) throws SQLException { throw unsupported(); }
    @Override public void updateLong(String columnLabel, long x) throws SQLException { throw unsupported(); }
    @Override public void updateFloat(String columnLabel, float x) throws SQLException { throw unsupported(); }
    @Override public void updateDouble(String columnLabel, double x) throws SQLException { throw unsupported(); }
    @Override public void updateBigDecimal(String columnLabel, BigDecimal x) throws SQLException { throw unsupported(); }
    @Override public void updateString(String columnLabel, String x) throws SQLException { throw unsupported(); }
    @Override public void updateBytes(String columnLabel, byte[] x) throws SQLException { throw unsupported(); }
    @Override public void updateDate(String columnLabel, Date x) throws SQLException { throw unsupported(); }
    @Override public void updateTime(String columnLabel, Time x) throws SQLException { throw unsupported(); }
    @Override public void updateTimestamp(String columnLabel, Timestamp x) throws SQLException { throw unsupported(); }
    @Override public void updateAsciiStream(String columnLabel, InputStream x, int length) throws SQLException { throw unsupported(); }
    @Override public void updateBinaryStream(String columnLabel, InputStream x, int length) throws SQLException { throw unsupported(); }
    @Override public void updateCharacterStream(String columnLabel, Reader x, int length) throws SQLException { throw unsupported(); }
    @Override public void updateObject(String columnLabel, Object x, int scaleOrLength) throws SQLException { throw unsupported(); }
    @Override public void updateObject(String columnLabel, Object x) throws SQLException { throw unsupported(); }
    @Override public void insertRow() throws SQLException { throw unsupported(); }
    @Override public void updateRow() throws SQLException { throw unsupported(); }
    @Override public void deleteRow() throws SQLException { throw unsupported(); }
    @Override public void refreshRow() throws SQLException { throw unsupported(); }
    @Override public void cancelRowUpdates() throws SQLException { throw unsupported(); }
    @Override public void moveToInsertRow() throws SQLException { throw unsupported(); }
    @Override public void moveToCurrentRow() throws SQLException { throw unsupported(); }
    @Override public Statement getStatement() { return owningStatement; }
    @Override public Object getObject(int columnIndex, Map<String, Class<?>> map) throws SQLException { throw unsupported(); }
    @Override public Ref getRef(int columnIndex) throws SQLException { throw unsupported(); }
    @Override public Blob getBlob(int columnIndex) throws SQLException { throw unsupported(); }
    @Override public Clob getClob(int columnIndex) throws SQLException { throw unsupported(); }
    @Override public Array getArray(int columnIndex) throws SQLException { throw unsupported(); }
    @Override public Object getObject(String columnLabel, Map<String, Class<?>> map) throws SQLException { throw unsupported(); }
    @Override public Ref getRef(String columnLabel) throws SQLException { throw unsupported(); }
    @Override public Blob getBlob(String columnLabel) throws SQLException { throw unsupported(); }
    @Override public Clob getClob(String columnLabel) throws SQLException { throw unsupported(); }
    @Override public Array getArray(String columnLabel) throws SQLException { throw unsupported(); }
    @Override public Date getDate(int columnIndex, Calendar cal) throws SQLException { throw unsupported(); }
    @Override public Date getDate(String columnLabel, Calendar cal) throws SQLException { throw unsupported(); }
    @Override public Time getTime(int columnIndex, Calendar cal) throws SQLException { throw unsupported(); }
    @Override public Time getTime(String columnLabel, Calendar cal) throws SQLException { throw unsupported(); }
    @Override public Timestamp getTimestamp(int columnIndex, Calendar cal) throws SQLException { throw unsupported(); }
    @Override public Timestamp getTimestamp(String columnLabel, Calendar cal) throws SQLException { throw unsupported(); }
    @Override public URL getURL(int columnIndex) throws SQLException { throw unsupported(); }
    @Override public URL getURL(String columnLabel) throws SQLException { throw unsupported(); }
    @Override public void updateRef(int columnIndex, Ref x) throws SQLException { throw unsupported(); }
    @Override public void updateRef(String columnLabel, Ref x) throws SQLException { throw unsupported(); }
    @Override public void updateBlob(int columnIndex, Blob x) throws SQLException { throw unsupported(); }
    @Override public void updateBlob(String columnLabel, Blob x) throws SQLException { throw unsupported(); }
    @Override public void updateClob(int columnIndex, Clob x) throws SQLException { throw unsupported(); }
    @Override public void updateClob(String columnLabel, Clob x) throws SQLException { throw unsupported(); }
    @Override public void updateArray(int columnIndex, Array x) throws SQLException { throw unsupported(); }
    @Override public void updateArray(String columnLabel, Array x) throws SQLException { throw unsupported(); }
    @Override public RowId getRowId(int columnIndex) throws SQLException { throw unsupported(); }
    @Override public RowId getRowId(String columnLabel) throws SQLException { throw unsupported(); }
    @Override public void updateRowId(int columnIndex, RowId x) throws SQLException { throw unsupported(); }
    @Override public void updateRowId(String columnLabel, RowId x) throws SQLException { throw unsupported(); }
    @Override public int getHoldability() { return ResultSet.CLOSE_CURSORS_AT_COMMIT; }
    @Override public void updateNString(int columnIndex, String nString) throws SQLException { throw unsupported(); }
    @Override public void updateNString(String columnLabel, String nString) throws SQLException { throw unsupported(); }
    @Override public void updateNClob(int columnIndex, NClob nClob) throws SQLException { throw unsupported(); }
    @Override public void updateNClob(String columnLabel, NClob nClob) throws SQLException { throw unsupported(); }
    @Override public NClob getNClob(int columnIndex) throws SQLException { throw unsupported(); }
    @Override public NClob getNClob(String columnLabel) throws SQLException { throw unsupported(); }
    @Override public SQLXML getSQLXML(int columnIndex) throws SQLException { throw unsupported(); }
    @Override public SQLXML getSQLXML(String columnLabel) throws SQLException { throw unsupported(); }
    @Override public void updateSQLXML(int columnIndex, SQLXML xmlObject) throws SQLException { throw unsupported(); }
    @Override public void updateSQLXML(String columnLabel, SQLXML xmlObject) throws SQLException { throw unsupported(); }
    @Override public String getNString(int columnIndex) throws SQLException { throw unsupported(); }
    @Override public String getNString(String columnLabel) throws SQLException { throw unsupported(); }
    @Override public Reader getNCharacterStream(int columnIndex) throws SQLException { throw unsupported(); }
    @Override public Reader getNCharacterStream(String columnLabel) throws SQLException { throw unsupported(); }
    @Override public void updateNCharacterStream(int columnIndex, Reader x, long length) throws SQLException { throw unsupported(); }
    @Override public void updateNCharacterStream(String columnLabel, Reader reader, long length) throws SQLException { throw unsupported(); }
    @Override public void updateAsciiStream(int columnIndex, InputStream x, long length) throws SQLException { throw unsupported(); }
    @Override public void updateBinaryStream(int columnIndex, InputStream x, long length) throws SQLException { throw unsupported(); }
    @Override public void updateCharacterStream(int columnIndex, Reader x, long length) throws SQLException { throw unsupported(); }
    @Override public void updateAsciiStream(String columnLabel, InputStream x, long length) throws SQLException { throw unsupported(); }
    @Override public void updateBinaryStream(String columnLabel, InputStream x, long length) throws SQLException { throw unsupported(); }
    @Override public void updateCharacterStream(String columnLabel, Reader reader, long length) throws SQLException { throw unsupported(); }
    @Override public void updateBlob(int columnIndex, InputStream inputStream, long length) throws SQLException { throw unsupported(); }
    @Override public void updateBlob(String columnLabel, InputStream inputStream, long length) throws SQLException { throw unsupported(); }
    @Override public void updateClob(int columnIndex, Reader reader, long length) throws SQLException { throw unsupported(); }
    @Override public void updateClob(String columnLabel, Reader reader, long length) throws SQLException { throw unsupported(); }
    @Override public void updateNClob(int columnIndex, Reader reader, long length) throws SQLException { throw unsupported(); }
    @Override public void updateNClob(String columnLabel, Reader reader, long length) throws SQLException { throw unsupported(); }
    @Override public void updateNCharacterStream(int columnIndex, Reader x) throws SQLException { throw unsupported(); }
    @Override public void updateNCharacterStream(String columnLabel, Reader reader) throws SQLException { throw unsupported(); }
    @Override public void updateAsciiStream(int columnIndex, InputStream x) throws SQLException { throw unsupported(); }
    @Override public void updateBinaryStream(int columnIndex, InputStream x) throws SQLException { throw unsupported(); }
    @Override public void updateCharacterStream(int columnIndex, Reader x) throws SQLException { throw unsupported(); }
    @Override public void updateAsciiStream(String columnLabel, InputStream x) throws SQLException { throw unsupported(); }
    @Override public void updateBinaryStream(String columnLabel, InputStream x) throws SQLException { throw unsupported(); }
    @Override public void updateCharacterStream(String columnLabel, Reader reader) throws SQLException { throw unsupported(); }
    @Override public void updateBlob(int columnIndex, InputStream inputStream) throws SQLException { throw unsupported(); }
    @Override public void updateBlob(String columnLabel, InputStream inputStream) throws SQLException { throw unsupported(); }
    @Override public void updateClob(int columnIndex, Reader reader) throws SQLException { throw unsupported(); }
    @Override public void updateClob(String columnLabel, Reader reader) throws SQLException { throw unsupported(); }
    @Override public void updateNClob(int columnIndex, Reader reader) throws SQLException { throw unsupported(); }
    @Override public void updateNClob(String columnLabel, Reader reader) throws SQLException { throw unsupported(); }

    @Override
    public <T> T getObject(int columnIndex, Class<T> type) throws SQLException { throw unsupported(); }

    @Override
    public <T> T getObject(String columnLabel, Class<T> type) throws SQLException { throw unsupported(); }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        if (iface.isInstance(this)) return iface.cast(this);
        throw new SQLException("Cannot unwrap to " + iface.getName());
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) {
        return iface.isInstance(this);
    }

    private static SQLFeatureNotSupportedException unsupported() {
        return new SQLFeatureNotSupportedException("Operation not supported by PaginatedResultSet");
    }
}
