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
import java.util.Arrays;
import java.util.Map;

/** Read-only JDBC array view of a VECTOR's FP32 coordinates. */
public final class VectorSqlArray implements Array {

    private float[] coordinates;

    private VectorSqlArray(float[] coordinates) {
        this.coordinates = coordinates.clone();
    }

    public static Array fromValue(Object value) throws SQLException {
        if (value == null) {
            return null;
        }
        if (value instanceof float[] vector) {
            return new VectorSqlArray(vector);
        }
        throw new SQLFeatureNotSupportedException(
                "JDBC getArray is supported only for VECTOR columns");
    }

    @Override
    public String getBaseTypeName() throws SQLException {
        requireOpen();
        return "REAL";
    }

    @Override
    public int getBaseType() throws SQLException {
        requireOpen();
        return Types.REAL;
    }

    @Override
    public Object getArray() throws SQLException {
        requireOpen();
        return coordinates.clone();
    }

    @Override
    public Object getArray(Map<String, Class<?>> map) throws SQLException {
        return getArray();
    }

    @Override
    public Object getArray(long index, int count) throws SQLException {
        requireOpen();
        if (index < 1 || count < 0 || index > coordinates.length + 1L
                || count > coordinates.length - (index - 1)) {
            throw new SQLException("VECTOR array slice is out of range");
        }
        int start = Math.toIntExact(index - 1);
        return Arrays.copyOfRange(coordinates, start, start + count);
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

    private void requireOpen() throws SQLException {
        if (coordinates == null) {
            throw new SQLException("VECTOR array has been freed");
        }
    }
}
