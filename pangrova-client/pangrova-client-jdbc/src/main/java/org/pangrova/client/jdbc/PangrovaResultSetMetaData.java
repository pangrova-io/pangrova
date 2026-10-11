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

import org.pangrova.transport.ColumnDescriptor;
import io.substrait.proto.Type;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.List;

/**
 * JDBC ResultSetMetaData backed by a list of {@link ColumnDescriptor}s.
 * Contains no Arrow imports — metadata is populated by {@code pangrova-transport}.
 */
public final class PangrovaResultSetMetaData implements ResultSetMetaData {

    /** Uniform display size reported for all columns. */
    private static final int DEFAULT_DISPLAY_SIZE = 20;

    private final List<ColumnDescriptor> columns;

    PangrovaResultSetMetaData(List<ColumnDescriptor> columns) {
        this.columns = List.copyOf(columns);
    }

    @Override
    public int getColumnCount() {
        return columns.size();
    }

    @Override
    public String getColumnName(int column) throws SQLException {
        return getColumn(column).name();
    }

    @Override
    public String getColumnLabel(int column) throws SQLException {
        return getColumnName(column);
    }

    @Override
    public int getColumnType(int column) throws SQLException {
        return getColumn(column).sqlType();
    }

    @Override
    public String getColumnTypeName(int column) throws SQLException {
        return getColumn(column).typeName();
    }

    @Override
    public String getColumnClassName(int column) throws SQLException {
        ColumnDescriptor descriptor = getColumn(column);
        if (descriptor.logicalType().isPresent()) {
            var child = descriptor.logicalType().orElseThrow().getSubstraitType().getList().getType();
            boolean nullable = (child.hasFp64() ? child.getFp64().getNullability() : child.getFp32().getNullability())
                    == Type.Nullability.NULLABILITY_NULLABLE;
            if (child.hasFp64()) return nullable ? Double[].class.getName() : double[].class.getName();
            return nullable ? Float[].class.getName() : float[].class.getName();
        }
        return switch (getColumnType(column)) {
            case Types.BOOLEAN -> Boolean.class.getName();
            case Types.INTEGER -> Integer.class.getName();
            case Types.BIGINT -> Long.class.getName();
            case Types.FLOAT, Types.DOUBLE -> Double.class.getName();
            case Types.VARCHAR -> String.class.getName();
            default -> Object.class.getName();
        };
    }

    @Override
    public boolean isAutoIncrement(int column) { return false; }

    @Override
    public boolean isCaseSensitive(int column) { return true; }

    @Override
    public boolean isSearchable(int column) { return true; }

    @Override
    public boolean isCurrency(int column) { return false; }

    @Override
    public int isNullable(int column) throws SQLException {
        var logical = getColumn(column).logicalType();
        return logical.isPresent() && logical.orElseThrow().getSubstraitType().getList().getNullability()
                == Type.Nullability.NULLABILITY_REQUIRED
                ? ResultSetMetaData.columnNoNulls : ResultSetMetaData.columnNullable;
    }

    @Override
    public boolean isSigned(int column) throws SQLException {
        int type = getColumnType(column);
        return type == Types.INTEGER || type == Types.BIGINT
                || type == Types.FLOAT || type == Types.DOUBLE;
    }

    @Override
    public int getColumnDisplaySize(int column) { return DEFAULT_DISPLAY_SIZE; }

    @Override
    public String getSchemaName(int column) { return ""; }

    @Override
    public int getPrecision(int column) throws SQLException {
        return getColumn(column).precision();
    }

    @Override
    public int getScale(int column) throws SQLException {
        return getColumn(column).scale();
    }

    @Override
    public String getTableName(int column) { return ""; }

    @Override
    public String getCatalogName(int column) { return ""; }

    @Override
    public boolean isReadOnly(int column) { return true; }

    @Override
    public boolean isWritable(int column) { return false; }

    @Override
    public boolean isDefinitelyWritable(int column) { return false; }

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

    private ColumnDescriptor getColumn(int column) throws SQLException {
        if (column < 1 || column > columns.size()) {
            throw new SQLException("Column index out of range: " + column
                    + ", column count: " + columns.size());
        }
        return columns.get(column - 1);
    }
}
