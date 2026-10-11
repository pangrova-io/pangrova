/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.pangrova.client.jdbc;

import org.pangrova.client.jdbc.value.VectorSqlArray;
import org.pangrova.transport.ColumnDescriptor;
import org.pangrova.transport.ResultBatch;

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
import java.util.Arrays;
import java.util.Calendar;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * JDBC ResultSet backed by a pre-decoded {@link ResultBatch}.
 * Contains no Arrow imports — Arrow decoding is done in {@code pangrova-transport}
 * before this ResultSet is constructed.
 *
 * <p>The complete result is currently held in heap memory. A bounded-memory
 * Flight-backed JDBC cursor is required before advertising large-result
 * streaming support.</p>
 */
public final class PangrovaResultSet implements ResultSet {

    private List<Object[]> rows;
    private List<ColumnDescriptor> columns;
    private final Statement owningStatement;
    private PangrovaResultSetMetaData metaData;
    // PERF-T19: Lazy-built column name index for O(1) findColumn() lookups.
    private Map<String, Integer> columnIndexCache;
    private int currentRowIndex = -1;
    private boolean closed;
    private boolean wasNull;

    PangrovaResultSet(ResultBatch batch, Statement owningStatement) {
        this.rows = batch.rows();
        this.columns = batch.columns();
        this.owningStatement = owningStatement;
    }

    @Override
    public boolean next() throws SQLException {
        checkOpen();
        currentRowIndex++;
        return currentRowIndex < rows.size();
    }

    @Override
    public void close() {
        closed = true;
        rows = List.of();
        columns = List.of();
        metaData = null;
        columnIndexCache = null;
    }

    @Override
    public boolean isClosed() {
        return closed;
    }

    @Override
    public ResultSetMetaData getMetaData() throws SQLException {
        checkOpen();
        if (metaData == null) {
            metaData = new PangrovaResultSetMetaData(columns);
        }
        return metaData;
    }

    @Override
    public boolean wasNull() {
        return wasNull;
    }

    // --- Type getters by column index (1-based) ---

    @Override
    public String getString(int columnIndex) throws SQLException {
        Object value = getObjectInternal(columnIndex);
        if (value == null) return null;
        if (value instanceof float[] vector) return Arrays.toString(vector);
        if (value instanceof double[] vector) return Arrays.toString(vector);
        if (value instanceof Float[] vector) return Arrays.toString(vector);
        if (value instanceof Double[] vector) return Arrays.toString(vector);
        return value.toString();
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

    // --- Type getters by column label ---

    @Override public String getString(String columnLabel) throws SQLException { return getString(findColumn(columnLabel)); }
    @Override public boolean getBoolean(String columnLabel) throws SQLException { return getBoolean(findColumn(columnLabel)); }
    @Override public int getInt(String columnLabel) throws SQLException { return getInt(findColumn(columnLabel)); }
    @Override public long getLong(String columnLabel) throws SQLException { return getLong(findColumn(columnLabel)); }
    @Override public double getDouble(String columnLabel) throws SQLException { return getDouble(findColumn(columnLabel)); }
    @Override public Object getObject(String columnLabel) throws SQLException { return getObject(findColumn(columnLabel)); }

    @Override
    public int findColumn(String columnLabel) throws SQLException {
        checkOpen();
        // PERF-T19: O(1) lookup via lazily-built HashMap instead of O(N) linear scan.
        Integer idx = columnIndex().get(columnLabel.toLowerCase());
        if (idx == null) {
            throw new SQLException("Column not found: " + columnLabel);
        }
        return idx;
    }

    private Map<String, Integer> columnIndex() {
        if (columnIndexCache == null) {
            Map<String, Integer> map = new HashMap<>(columns.size() * 2);
            for (int i = 0; i < columns.size(); i++) {
                // putIfAbsent: first column wins for duplicate names (JDBC convention)
                map.putIfAbsent(columns.get(i).name().toLowerCase(), i + 1);
            }
            columnIndexCache = map;
        }
        return columnIndexCache;
    }

    // --- Internal value extraction ---

    private Object getObjectInternal(int columnIndex) throws SQLException {
        checkOpen();
        checkRow();
        if (columnIndex < 1 || columnIndex > columns.size()) {
            throw new SQLException("Column index out of range: " + columnIndex);
        }
        Object[] row = rows.get(currentRowIndex);
        Object value = row[columnIndex - 1];
        wasNull = (value == null);
        return value;
    }

    private void checkOpen() throws SQLException {
        if (closed) throw new SQLException("ResultSet is closed");
    }

    private void checkRow() throws SQLException {
        if (currentRowIndex < 0 || currentRowIndex >= rows.size()) {
            throw new SQLException("Invalid cursor position");
        }
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
    @Override public boolean isBeforeFirst() { return currentRowIndex < 0 && !rows.isEmpty(); }
    @Override public boolean isAfterLast() { return currentRowIndex >= rows.size(); }
    @Override public boolean isFirst() { return currentRowIndex == 0; }
    @Override public boolean isLast() { return currentRowIndex == rows.size() - 1; }
    @Override public void beforeFirst() throws SQLException { throw unsupported(); }
    @Override public void afterLast() throws SQLException { throw unsupported(); }
    @Override public boolean first() throws SQLException { throw unsupported(); }
    @Override public boolean last() throws SQLException { throw unsupported(); }
    @Override public int getRow() { return currentRowIndex >= 0 && currentRowIndex < rows.size() ? currentRowIndex + 1 : 0; }
    @Override public boolean absolute(int row) throws SQLException { throw unsupported(); }
    @Override public boolean relative(int rowOffset) throws SQLException { throw unsupported(); }
    @Override public boolean previous() throws SQLException { throw unsupported(); }
    @Override public void setFetchDirection(int direction) throws SQLException { throw unsupported(); }
    @Override public int getFetchDirection() { return ResultSet.FETCH_FORWARD; }
    @Override public void setFetchSize(int fetchSize) {}
    @Override public int getFetchSize() { return 0; }
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
    @Override public Statement getStatement() throws SQLException {
        checkOpen();
        return owningStatement;
    }
    @Override public Object getObject(int columnIndex, Map<String, Class<?>> map) throws SQLException { throw unsupported(); }
    @Override public Ref getRef(int columnIndex) throws SQLException { throw unsupported(); }
    @Override public Blob getBlob(int columnIndex) throws SQLException { throw unsupported(); }
    @Override public Clob getClob(int columnIndex) throws SQLException { throw unsupported(); }
    @Override public Array getArray(int columnIndex) throws SQLException {
        return VectorSqlArray.fromValue(getObjectInternal(columnIndex));
    }
    @Override public Object getObject(String columnLabel, Map<String, Class<?>> map) throws SQLException { throw unsupported(); }
    @Override public Ref getRef(String columnLabel) throws SQLException { throw unsupported(); }
    @Override public Blob getBlob(String columnLabel) throws SQLException { throw unsupported(); }
    @Override public Clob getClob(String columnLabel) throws SQLException { throw unsupported(); }
    @Override public Array getArray(String columnLabel) throws SQLException {
        return getArray(findColumn(columnLabel));
    }
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
    @Override public <T> T getObject(int columnIndex, Class<T> type) throws SQLException { throw unsupported(); }
    @Override public <T> T getObject(String columnLabel, Class<T> type) throws SQLException { throw unsupported(); }

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
        return new SQLFeatureNotSupportedException("Operation not supported by PangrovaResultSet");
    }
}
