package org.pangrova.client.jdbc;

import java.sql.ParameterMetaData;
import java.sql.SQLException;

/**
 * Minimal ParameterMetaData implementation for BI tool compatibility.
 *
 * <p>Since Pangrova uses client-side parameter substitution (no server-side prepare),
 * type information is not available from the server. This implementation reports
 * parameter count and uses VARCHAR as the default type for all parameters.</p>
 */
final class PangrovaParameterMetaData implements ParameterMetaData {

    private final int parameterCount;

    PangrovaParameterMetaData(int parameterCount) {
        this.parameterCount = parameterCount;
    }

    @Override
    public int getParameterCount() {
        return parameterCount;
    }

    @Override
    public int isNullable(int param) throws SQLException {
        checkParam(param);
        return ParameterMetaData.parameterNullable;
    }

    @Override
    public boolean isSigned(int param) throws SQLException {
        checkParam(param);
        return true;
    }

    @Override
    public int getPrecision(int param) throws SQLException {
        checkParam(param);
        return 0;
    }

    @Override
    public int getScale(int param) throws SQLException {
        checkParam(param);
        return 0;
    }

    @Override
    public int getParameterType(int param) throws SQLException {
        checkParam(param);
        return java.sql.Types.VARCHAR;
    }

    @Override
    public String getParameterTypeName(int param) throws SQLException {
        checkParam(param);
        return "VARCHAR";
    }

    @Override
    public String getParameterClassName(int param) throws SQLException {
        checkParam(param);
        return String.class.getName();
    }

    @Override
    public int getParameterMode(int param) throws SQLException {
        checkParam(param);
        return ParameterMetaData.parameterModeIn;
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        if (iface.isInstance(this)) return iface.cast(this);
        throw new SQLException("Cannot unwrap to " + iface.getName());
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) {
        return iface.isInstance(this);
    }

    private void checkParam(int param) throws SQLException {
        if (param < 1 || param > parameterCount) {
            throw new SQLException("Parameter index " + param + " out of range [1, " + parameterCount + "]");
        }
    }
}
