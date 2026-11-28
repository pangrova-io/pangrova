package org.pangrova.persistence.query.postgresql.state;

import java.sql.Array;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

/** Owns the JDBC arrays bound to one fixed-shape journal statement. */
final class JournalSqlArrays implements AutoCloseable {
    private final Connection connection;
    private final List<Array> arrays = new ArrayList<>();

    JournalSqlArrays(Connection connection) { this.connection = connection; }

    void bind(PreparedStatement statement, int parameter, String type, Object[] values) throws SQLException {
        var array = connection.createArrayOf(type, values);
        arrays.add(array);
        statement.setArray(parameter, array);
    }

    @Override
    public void close() throws SQLException {
        SQLException failure = null;
        for (var array : arrays) {
            try {
                array.free();
            } catch (SQLException release) {
                if (failure == null) failure = release;
                else failure.addSuppressed(release);
            }
        }
        if (failure != null) throw failure;
    }
}
