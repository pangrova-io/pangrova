package org.pangrova.client.jdbc;

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
import java.sql.Types;
import java.util.Calendar;
import java.util.List;
import java.util.Map;

/**
 * In-memory ResultSet backed by a list of Object arrays.
 * Used for DatabaseMetaData results (getCatalogs, getSchemas, getTables, getColumns, etc.)
 * where data is fetched from REST and returned as a standard JDBC ResultSet.
 *
 * <p>Forward-only, read-only. Column indices are 1-based per JDBC spec.</p>
 */
final class StaticResultSet implements ResultSet {

    private final List<String> columnNames;
    private final List<Integer> columnTypes;
    private final List<Object[]> rows;
    private int cursor = -1;
    private boolean closed;
    private boolean wasNull;

    StaticResultSet(List<String> columnNames, List<Integer> columnTypes, List<Object[]> rows) {
        this.columnNames = List.copyOf(columnNames);
        this.columnTypes = List.copyOf(columnTypes);
        this.rows = List.copyOf(rows);
    }

    @Override
    public boolean next() throws SQLException {
        checkOpen();
        cursor++;
        return cursor < rows.size();
    }

    @Override
    public void close() { closed = true; }

    @Override
    public boolean isClosed() { return closed; }

    @Override
    public ResultSetMetaData getMetaData() {
        return new StaticResultSetMetaData(columnNames, columnTypes);
    }

    @Override
    public boolean wasNull() { return wasNull; }

    @Override
    public String getString(int columnIndex) throws SQLException {
        Object val = getValueAt(columnIndex);
        if (val == null) {
            wasNull = true;
            return null;
        }
        wasNull = false;
        return val.toString();
    }

    @Override
    public int getInt(int columnIndex) throws SQLException {
        Object val = getValueAt(columnIndex);
        if (val == null) {
            wasNull = true;
            return 0;
        }
        wasNull = false;
        if (val instanceof Number n) return n.intValue();
        return Integer.parseInt(val.toString());
    }

    @Override
    public short getShort(int columnIndex) throws SQLException {
        Object val = getValueAt(columnIndex);
        if (val == null) {
            wasNull = true;
            return 0;
        }
        wasNull = false;
        if (val instanceof Number n) return n.shortValue();
        return Short.parseShort(val.toString());
    }

    @Override
    public long getLong(int columnIndex) throws SQLException {
        Object val = getValueAt(columnIndex);
        if (val == null) {
            wasNull = true;
            return 0L;
        }
        wasNull = false;
        if (val instanceof Number n) return n.longValue();
        return Long.parseLong(val.toString());
    }

    @Override
    public boolean getBoolean(int columnIndex) throws SQLException {
        Object val = getValueAt(columnIndex);
        if (val == null) {
            wasNull = true;
            return false;
        }
        wasNull = false;
        if (val instanceof Boolean b) return b;
        return Boolean.parseBoolean(val.toString());
    }

    @Override
    public Object getObject(int columnIndex) throws SQLException {
        Object val = getValueAt(columnIndex);
        wasNull = (val == null);
        return val;
    }

    @Override public String getString(String columnLabel) throws SQLException { return getString(findColumn(columnLabel)); }
    @Override public int getInt(String columnLabel) throws SQLException { return getInt(findColumn(columnLabel)); }
    @Override public short getShort(String columnLabel) throws SQLException { return getShort(findColumn(columnLabel)); }
    @Override public long getLong(String columnLabel) throws SQLException { return getLong(findColumn(columnLabel)); }
    @Override public boolean getBoolean(String columnLabel) throws SQLException { return getBoolean(findColumn(columnLabel)); }
    @Override public Object getObject(String columnLabel) throws SQLException { return getObject(findColumn(columnLabel)); }

    @Override
    public int findColumn(String columnLabel) throws SQLException {
        for (int i = 0; i < columnNames.size(); i++) {
            if (columnNames.get(i).equalsIgnoreCase(columnLabel)) return i + 1;
        }
        throw new SQLException("Column not found: " + columnLabel);
    }

    private Object getValueAt(int columnIndex) throws SQLException {
        checkOpen();
        if (cursor < 0 || cursor >= rows.size()) {
            throw new SQLException("Invalid cursor position");
        }
        if (columnIndex < 1 || columnIndex > columnNames.size()) {
            throw new SQLException("Column index out of range: " + columnIndex);
        }
        return rows.get(cursor)[columnIndex - 1];
    }

    private void checkOpen() throws SQLException {
        if (closed) throw new SQLException("ResultSet is closed");
    }

    // --- Navigation (forward-only) ---
    @Override public boolean isBeforeFirst() { return cursor < 0; }
    @Override public boolean isAfterLast() { return cursor >= rows.size(); }
    @Override public boolean isFirst() { return cursor == 0 && !rows.isEmpty(); }
    @Override public boolean isLast() { return cursor == rows.size() - 1 && !rows.isEmpty(); }
    @Override public int getRow() { return cursor >= 0 && cursor < rows.size() ? cursor + 1 : 0; }
    @Override public int getType() { return ResultSet.TYPE_FORWARD_ONLY; }
    @Override public int getConcurrency() { return ResultSet.CONCUR_READ_ONLY; }
    @Override public int getHoldability() { return ResultSet.CLOSE_CURSORS_AT_COMMIT; }
    @Override public int getFetchDirection() { return ResultSet.FETCH_FORWARD; }
    @Override public void setFetchDirection(int direction) {}
    @Override public void setFetchSize(int fetchSize) {}
    @Override public int getFetchSize() { return 0; }
    @Override public SQLWarning getWarnings() { return null; }
    @Override public void clearWarnings() {}
    @Override public Statement getStatement() { return null; }
    @Override public boolean rowUpdated() { return false; }
    @Override public boolean rowInserted() { return false; }
    @Override public boolean rowDeleted() { return false; }
    @Override public String getCursorName() throws SQLException { throw unsupported(); }

    // --- Unsupported type getters ---
    @Override public byte getByte(int c) throws SQLException { throw unsupported(); }
    @Override public float getFloat(int c) throws SQLException { throw unsupported(); }
    @Override public double getDouble(int c) throws SQLException { throw unsupported(); }
    @Override public BigDecimal getBigDecimal(int c, int s) throws SQLException { throw unsupported(); }
    @Override public BigDecimal getBigDecimal(int c) throws SQLException { throw unsupported(); }
    @Override public byte[] getBytes(int c) throws SQLException { throw unsupported(); }
    @Override public Date getDate(int c) throws SQLException { throw unsupported(); }
    @Override public Time getTime(int c) throws SQLException { throw unsupported(); }
    @Override public Timestamp getTimestamp(int c) throws SQLException { throw unsupported(); }
    @Override public InputStream getAsciiStream(int c) throws SQLException { throw unsupported(); }
    @Override public InputStream getUnicodeStream(int c) throws SQLException { throw unsupported(); }
    @Override public InputStream getBinaryStream(int c) throws SQLException { throw unsupported(); }
    @Override public byte getByte(String l) throws SQLException { throw unsupported(); }
    @Override public float getFloat(String l) throws SQLException { throw unsupported(); }
    @Override public double getDouble(String l) throws SQLException { throw unsupported(); }
    @Override public BigDecimal getBigDecimal(String l, int s) throws SQLException { throw unsupported(); }
    @Override public BigDecimal getBigDecimal(String l) throws SQLException { throw unsupported(); }
    @Override public byte[] getBytes(String l) throws SQLException { throw unsupported(); }
    @Override public Date getDate(String l) throws SQLException { throw unsupported(); }
    @Override public Time getTime(String l) throws SQLException { throw unsupported(); }
    @Override public Timestamp getTimestamp(String l) throws SQLException { throw unsupported(); }
    @Override public InputStream getAsciiStream(String l) throws SQLException { throw unsupported(); }
    @Override public InputStream getUnicodeStream(String l) throws SQLException { throw unsupported(); }
    @Override public InputStream getBinaryStream(String l) throws SQLException { throw unsupported(); }
    @Override public Reader getCharacterStream(int c) throws SQLException { throw unsupported(); }
    @Override public Reader getCharacterStream(String l) throws SQLException { throw unsupported(); }

    // --- Unsupported navigation ---
    @Override public void beforeFirst() throws SQLException { throw unsupported(); }
    @Override public void afterLast() throws SQLException { throw unsupported(); }
    @Override public boolean first() throws SQLException { throw unsupported(); }
    @Override public boolean last() throws SQLException { throw unsupported(); }
    @Override public boolean absolute(int row) throws SQLException { throw unsupported(); }
    @Override public boolean relative(int rowOffset) throws SQLException { throw unsupported(); }
    @Override public boolean previous() throws SQLException { throw unsupported(); }

    // --- Unsupported updates ---
    @Override public void updateNull(int c) throws SQLException { throw unsupported(); }
    @Override public void updateBoolean(int c, boolean x) throws SQLException { throw unsupported(); }
    @Override public void updateByte(int c, byte x) throws SQLException { throw unsupported(); }
    @Override public void updateShort(int c, short x) throws SQLException { throw unsupported(); }
    @Override public void updateInt(int c, int x) throws SQLException { throw unsupported(); }
    @Override public void updateLong(int c, long x) throws SQLException { throw unsupported(); }
    @Override public void updateFloat(int c, float x) throws SQLException { throw unsupported(); }
    @Override public void updateDouble(int c, double x) throws SQLException { throw unsupported(); }
    @Override public void updateBigDecimal(int c, BigDecimal x) throws SQLException { throw unsupported(); }
    @Override public void updateString(int c, String x) throws SQLException { throw unsupported(); }
    @Override public void updateBytes(int c, byte[] x) throws SQLException { throw unsupported(); }
    @Override public void updateDate(int c, Date x) throws SQLException { throw unsupported(); }
    @Override public void updateTime(int c, Time x) throws SQLException { throw unsupported(); }
    @Override public void updateTimestamp(int c, Timestamp x) throws SQLException { throw unsupported(); }
    @Override public void updateAsciiStream(int c, InputStream x, int l) throws SQLException { throw unsupported(); }
    @Override public void updateBinaryStream(int c, InputStream x, int l) throws SQLException { throw unsupported(); }
    @Override public void updateCharacterStream(int c, Reader x, int l) throws SQLException { throw unsupported(); }
    @Override public void updateObject(int c, Object x, int s) throws SQLException { throw unsupported(); }
    @Override public void updateObject(int c, Object x) throws SQLException { throw unsupported(); }
    @Override public void updateNull(String l) throws SQLException { throw unsupported(); }
    @Override public void updateBoolean(String l, boolean x) throws SQLException { throw unsupported(); }
    @Override public void updateByte(String l, byte x) throws SQLException { throw unsupported(); }
    @Override public void updateShort(String l, short x) throws SQLException { throw unsupported(); }
    @Override public void updateInt(String l, int x) throws SQLException { throw unsupported(); }
    @Override public void updateLong(String l, long x) throws SQLException { throw unsupported(); }
    @Override public void updateFloat(String l, float x) throws SQLException { throw unsupported(); }
    @Override public void updateDouble(String l, double x) throws SQLException { throw unsupported(); }
    @Override public void updateBigDecimal(String l, BigDecimal x) throws SQLException { throw unsupported(); }
    @Override public void updateString(String l, String x) throws SQLException { throw unsupported(); }
    @Override public void updateBytes(String l, byte[] x) throws SQLException { throw unsupported(); }
    @Override public void updateDate(String l, Date x) throws SQLException { throw unsupported(); }
    @Override public void updateTime(String l, Time x) throws SQLException { throw unsupported(); }
    @Override public void updateTimestamp(String l, Timestamp x) throws SQLException { throw unsupported(); }
    @Override public void updateAsciiStream(String l, InputStream x, int len) throws SQLException { throw unsupported(); }
    @Override public void updateBinaryStream(String l, InputStream x, int len) throws SQLException { throw unsupported(); }
    @Override public void updateCharacterStream(String l, Reader x, int len) throws SQLException { throw unsupported(); }
    @Override public void updateObject(String l, Object x, int s) throws SQLException { throw unsupported(); }
    @Override public void updateObject(String l, Object x) throws SQLException { throw unsupported(); }
    @Override public void insertRow() throws SQLException { throw unsupported(); }
    @Override public void updateRow() throws SQLException { throw unsupported(); }
    @Override public void deleteRow() throws SQLException { throw unsupported(); }
    @Override public void refreshRow() throws SQLException { throw unsupported(); }
    @Override public void cancelRowUpdates() throws SQLException { throw unsupported(); }
    @Override public void moveToInsertRow() throws SQLException { throw unsupported(); }
    @Override public void moveToCurrentRow() throws SQLException { throw unsupported(); }

    // --- Unsupported complex types ---
    @Override public Object getObject(int c, Map<String, Class<?>> m) throws SQLException { throw unsupported(); }
    @Override public Ref getRef(int c) throws SQLException { throw unsupported(); }
    @Override public Blob getBlob(int c) throws SQLException { throw unsupported(); }
    @Override public Clob getClob(int c) throws SQLException { throw unsupported(); }
    @Override public Array getArray(int c) throws SQLException { throw unsupported(); }
    @Override public Object getObject(String l, Map<String, Class<?>> m) throws SQLException { throw unsupported(); }
    @Override public Ref getRef(String l) throws SQLException { throw unsupported(); }
    @Override public Blob getBlob(String l) throws SQLException { throw unsupported(); }
    @Override public Clob getClob(String l) throws SQLException { throw unsupported(); }
    @Override public Array getArray(String l) throws SQLException { throw unsupported(); }
    @Override public Date getDate(int c, Calendar cal) throws SQLException { throw unsupported(); }
    @Override public Date getDate(String l, Calendar cal) throws SQLException { throw unsupported(); }
    @Override public Time getTime(int c, Calendar cal) throws SQLException { throw unsupported(); }
    @Override public Time getTime(String l, Calendar cal) throws SQLException { throw unsupported(); }
    @Override public Timestamp getTimestamp(int c, Calendar cal) throws SQLException { throw unsupported(); }
    @Override public Timestamp getTimestamp(String l, Calendar cal) throws SQLException { throw unsupported(); }
    @Override public URL getURL(int c) throws SQLException { throw unsupported(); }
    @Override public URL getURL(String l) throws SQLException { throw unsupported(); }
    @Override public RowId getRowId(int c) throws SQLException { throw unsupported(); }
    @Override public RowId getRowId(String l) throws SQLException { throw unsupported(); }
    @Override public NClob getNClob(int c) throws SQLException { throw unsupported(); }
    @Override public NClob getNClob(String l) throws SQLException { throw unsupported(); }
    @Override public SQLXML getSQLXML(int c) throws SQLException { throw unsupported(); }
    @Override public SQLXML getSQLXML(String l) throws SQLException { throw unsupported(); }
    @Override public String getNString(int c) throws SQLException { throw unsupported(); }
    @Override public String getNString(String l) throws SQLException { throw unsupported(); }
    @Override public Reader getNCharacterStream(int c) throws SQLException { throw unsupported(); }
    @Override public Reader getNCharacterStream(String l) throws SQLException { throw unsupported(); }
    @Override public <T> T getObject(int c, Class<T> t) throws SQLException { throw unsupported(); }
    @Override public <T> T getObject(String l, Class<T> t) throws SQLException { throw unsupported(); }

    // --- Unsupported update streams ---
    @Override public void updateRef(int c, Ref x) throws SQLException { throw unsupported(); }
    @Override public void updateRef(String l, Ref x) throws SQLException { throw unsupported(); }
    @Override public void updateBlob(int c, Blob x) throws SQLException { throw unsupported(); }
    @Override public void updateBlob(String l, Blob x) throws SQLException { throw unsupported(); }
    @Override public void updateClob(int c, Clob x) throws SQLException { throw unsupported(); }
    @Override public void updateClob(String l, Clob x) throws SQLException { throw unsupported(); }
    @Override public void updateArray(int c, Array x) throws SQLException { throw unsupported(); }
    @Override public void updateArray(String l, Array x) throws SQLException { throw unsupported(); }
    @Override public void updateRowId(int c, RowId x) throws SQLException { throw unsupported(); }
    @Override public void updateRowId(String l, RowId x) throws SQLException { throw unsupported(); }
    @Override public void updateNString(int c, String x) throws SQLException { throw unsupported(); }
    @Override public void updateNString(String l, String x) throws SQLException { throw unsupported(); }
    @Override public void updateNClob(int c, NClob x) throws SQLException { throw unsupported(); }
    @Override public void updateNClob(String l, NClob x) throws SQLException { throw unsupported(); }
    @Override public void updateSQLXML(int c, SQLXML x) throws SQLException { throw unsupported(); }
    @Override public void updateSQLXML(String l, SQLXML x) throws SQLException { throw unsupported(); }
    @Override public void updateNCharacterStream(int c, Reader x, long len) throws SQLException { throw unsupported(); }
    @Override public void updateNCharacterStream(String l, Reader x, long len) throws SQLException { throw unsupported(); }
    @Override public void updateAsciiStream(int c, InputStream x, long len) throws SQLException { throw unsupported(); }
    @Override public void updateBinaryStream(int c, InputStream x, long len) throws SQLException { throw unsupported(); }
    @Override public void updateCharacterStream(int c, Reader x, long len) throws SQLException { throw unsupported(); }
    @Override public void updateAsciiStream(String l, InputStream x, long len) throws SQLException { throw unsupported(); }
    @Override public void updateBinaryStream(String l, InputStream x, long len) throws SQLException { throw unsupported(); }
    @Override public void updateCharacterStream(String l, Reader x, long len) throws SQLException { throw unsupported(); }
    @Override public void updateBlob(int c, InputStream x, long len) throws SQLException { throw unsupported(); }
    @Override public void updateBlob(String l, InputStream x, long len) throws SQLException { throw unsupported(); }
    @Override public void updateClob(int c, Reader x, long len) throws SQLException { throw unsupported(); }
    @Override public void updateClob(String l, Reader x, long len) throws SQLException { throw unsupported(); }
    @Override public void updateNClob(int c, Reader x, long len) throws SQLException { throw unsupported(); }
    @Override public void updateNClob(String l, Reader x, long len) throws SQLException { throw unsupported(); }
    @Override public void updateNCharacterStream(int c, Reader x) throws SQLException { throw unsupported(); }
    @Override public void updateNCharacterStream(String l, Reader x) throws SQLException { throw unsupported(); }
    @Override public void updateAsciiStream(int c, InputStream x) throws SQLException { throw unsupported(); }
    @Override public void updateBinaryStream(int c, InputStream x) throws SQLException { throw unsupported(); }
    @Override public void updateCharacterStream(int c, Reader x) throws SQLException { throw unsupported(); }
    @Override public void updateAsciiStream(String l, InputStream x) throws SQLException { throw unsupported(); }
    @Override public void updateBinaryStream(String l, InputStream x) throws SQLException { throw unsupported(); }
    @Override public void updateCharacterStream(String l, Reader x) throws SQLException { throw unsupported(); }
    @Override public void updateBlob(int c, InputStream x) throws SQLException { throw unsupported(); }
    @Override public void updateBlob(String l, InputStream x) throws SQLException { throw unsupported(); }
    @Override public void updateClob(int c, Reader x) throws SQLException { throw unsupported(); }
    @Override public void updateClob(String l, Reader x) throws SQLException { throw unsupported(); }
    @Override public void updateNClob(int c, Reader x) throws SQLException { throw unsupported(); }
    @Override public void updateNClob(String l, Reader x) throws SQLException { throw unsupported(); }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        if (iface.isInstance(this)) return iface.cast(this);
        throw new SQLException("Cannot unwrap to " + iface.getName());
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) { return iface.isInstance(this); }

    private static SQLFeatureNotSupportedException unsupported() {
        return new SQLFeatureNotSupportedException("Operation not supported by metadata ResultSet");
    }

    /**
     * Minimal ResultSetMetaData for static metadata result sets.
     */
    private record StaticResultSetMetaData(
            List<String> names, List<Integer> types) implements ResultSetMetaData {

        @Override public int getColumnCount() { return names.size(); }
        @Override public String getColumnName(int column) { return names.get(column - 1); }
        @Override public String getColumnLabel(int column) { return names.get(column - 1); }
        @Override public int getColumnType(int column) { return types.get(column - 1); }
        @Override public String getColumnTypeName(int column) {
            return switch (types.get(column - 1)) {
                case Types.VARCHAR -> "VARCHAR";
                case Types.INTEGER -> "INTEGER";
                case Types.SMALLINT -> "SMALLINT";
                case Types.BOOLEAN -> "BOOLEAN";
                default -> "OTHER";
            };
        }
        @Override public boolean isAutoIncrement(int column) { return false; }
        @Override public boolean isCaseSensitive(int column) { return true; }
        @Override public boolean isSearchable(int column) { return true; }
        @Override public boolean isCurrency(int column) { return false; }
        @Override public int isNullable(int column) { return ResultSetMetaData.columnNullable; }
        @Override public boolean isSigned(int column) { return types.get(column - 1) == Types.INTEGER; }
        @Override public int getColumnDisplaySize(int column) { return 256; }
        @Override public String getSchemaName(int column) { return ""; }
        @Override public int getPrecision(int column) { return 0; }
        @Override public int getScale(int column) { return 0; }
        @Override public String getTableName(int column) { return ""; }
        @Override public String getCatalogName(int column) { return ""; }
        @Override public boolean isReadOnly(int column) { return true; }
        @Override public boolean isWritable(int column) { return false; }
        @Override public boolean isDefinitelyWritable(int column) { return false; }
        @Override public String getColumnClassName(int column) { return String.class.getName(); }
        @Override public <T> T unwrap(Class<T> iface) throws SQLException {
            if (iface.isInstance(this)) return iface.cast(this);
            throw new SQLException("Cannot unwrap");
        }
        @Override public boolean isWrapperFor(Class<?> iface) { return iface.isInstance(this); }
    }
}
