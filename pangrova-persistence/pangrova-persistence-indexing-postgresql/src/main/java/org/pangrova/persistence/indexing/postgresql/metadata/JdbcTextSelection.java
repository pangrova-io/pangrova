package org.pangrova.persistence.indexing.postgresql.metadata;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;

/** Owns one PostgreSQL text-array parameter until its statement and result have closed. */
final class JdbcTextSelection implements AutoCloseable {
    private final java.sql.Array array;

    private JdbcTextSelection(java.sql.Array array) {
        this.array = array;
    }

    java.sql.Array array() {
        return array;
    }

    static JdbcTextSelection create(Connection connection, List<String> values) throws SQLException {
        return new JdbcTextSelection(connection.createArrayOf("text", values.toArray(String[]::new)));
    }

    @Override
    public void close() throws SQLException {
        array.free();
    }
}
