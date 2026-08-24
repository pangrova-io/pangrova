package org.pangrova.metadata.store;

import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * Shared JDBC functional interfaces for metadata store implementations.
 *
 * <p>Provides {@link SqlConsumer} and {@link SqlFunction} to eliminate
 * duplicate definitions across {@code store} and {@code dynamictable} packages.</p>
 */
public final class JdbcSupport {

    private JdbcSupport() {}

    @FunctionalInterface
    public interface SqlConsumer {
        void accept(PreparedStatement ps) throws SQLException;
    }

    @FunctionalInterface
    public interface SqlFunction<T, R> {
        R apply(T input) throws SQLException;
    }
}
