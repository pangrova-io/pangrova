package org.pangrova.streaming.jdbc;

import java.sql.PreparedStatement;
import java.sql.SQLException;

/** Shared JDBC functional interfaces for streaming state stores. */
public final class JdbcStreamingSupport {

    private JdbcStreamingSupport() {}

    @FunctionalInterface
    public interface SqlConsumer {
        void accept(PreparedStatement statement) throws SQLException;
    }

    @FunctionalInterface
    public interface SqlFunction<T, R> {
        R apply(T input) throws SQLException;
    }
}
