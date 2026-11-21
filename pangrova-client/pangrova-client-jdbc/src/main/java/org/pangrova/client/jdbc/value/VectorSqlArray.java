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
package org.pangrova.client.jdbc.value;

import java.sql.Array;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Types;
import java.util.Map;

/** Read-only JDBC array view of a VECTOR's Float32 or Float64 coordinates. */
public final class VectorSqlArray implements Array {

    private Object coordinates;

    private VectorSqlArray(Object coordinates) {
        this.coordinates = copyCoordinates(coordinates);
    }

    /** Creates an independently owned array of VECTOR coordinates, preserving SQL NULL. */
    public static Array fromValue(Object value) throws SQLException {
        if (value == null) {
            return null;
        }
        if (value instanceof float[] || value instanceof double[]
                || value instanceof Float[] || value instanceof Double[]) {
            return new VectorSqlArray(value);
        }
        throw new SQLFeatureNotSupportedException(
                "JDBC getArray is supported only for VECTOR columns");
    }

    @Override
    public String getBaseTypeName() throws SQLException {
        requireOpen();
        return isDoublePrecision() ? "DOUBLE" : "REAL";
    }

    @Override
    public int getBaseType() throws SQLException {
        requireOpen();
        return isDoublePrecision() ? Types.DOUBLE : Types.REAL;
    }

    @Override
    public Object getArray() throws SQLException {
        requireOpen();
        return copyCoordinates(coordinates);
    }

    @Override
    public Object getArray(Map<String, Class<?>> map) throws SQLException {
        return getArray();
    }

    @Override
    public Object getArray(long index, int count) throws SQLException {
        requireOpen();
        int length = java.lang.reflect.Array.getLength(coordinates);
        if (index < 1 || count < 0 || index > length + 1L
                || count > length - (index - 1)) {
            throw new SQLException("VECTOR array slice is out of range");
        }
        int start = Math.toIntExact(index - 1);
        return copyCoordinates(coordinates, start, count);
    }

    @Override
    public Object getArray(long index, int count, Map<String, Class<?>> map)
            throws SQLException {
        return getArray(index, count);
    }

    @Override
    public ResultSet getResultSet() throws SQLException {
        throw new SQLFeatureNotSupportedException("VECTOR array result set is unsupported");
    }

    @Override
    public ResultSet getResultSet(Map<String, Class<?>> map) throws SQLException {
        return getResultSet();
    }

    @Override
    public ResultSet getResultSet(long index, int count) throws SQLException {
        return getResultSet();
    }

    @Override
    public ResultSet getResultSet(long index, int count,
            Map<String, Class<?>> map) throws SQLException {
        return getResultSet();
    }

    @Override
    public void free() {
        coordinates = null;
    }

    private static Object copyCoordinates(Object values) {
        return copyCoordinates(values, 0, java.lang.reflect.Array.getLength(values));
    }

    private static Object copyCoordinates(Object values, int start, int count) {
        Object result = java.lang.reflect.Array.newInstance(values.getClass().getComponentType(), count);
        System.arraycopy(values, start, result, 0, count);
        return result;
    }

    private boolean isDoublePrecision() {
        return coordinates instanceof double[] || coordinates instanceof Double[];
    }

    private void requireOpen() throws SQLException {
        if (coordinates == null) {
            throw new SQLException("VECTOR array has been freed");
        }
    }
}
