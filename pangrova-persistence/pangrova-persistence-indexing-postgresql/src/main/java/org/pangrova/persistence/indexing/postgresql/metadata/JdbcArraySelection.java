package org.pangrova.persistence.indexing.postgresql.metadata;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;

/** Owns one PostgreSQL array parameter until its statement and result have closed. */
final class JdbcArraySelection implements AutoCloseable {
    private final java.sql.Array array;

    private JdbcArraySelection(java.sql.Array array) {
        this.array = array;
    }

    java.sql.Array array() {
        return array;
    }

    static JdbcArraySelection create(Connection connection, List<String> values) throws SQLException {
        return create(connection, "text", values.toArray(String[]::new));
    }

    static JdbcArraySelection create(Connection connection, String type, Object[] values) throws SQLException {
        return new JdbcArraySelection(connection.createArrayOf(type, values));
    }

    @Override
    public void close() throws SQLException {
        array.free();
    }
}
