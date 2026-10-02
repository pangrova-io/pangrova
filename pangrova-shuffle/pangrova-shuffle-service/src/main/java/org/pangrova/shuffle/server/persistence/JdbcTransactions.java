package org.pangrova.shuffle.server.persistence;

import java.sql.Connection;
import java.sql.SQLException;

import javax.sql.DataSource;

/** Executes PostgreSQL work with explicit commit and rollback semantics. */
public final class JdbcTransactions {

    private JdbcTransactions() {}

    public static <T> T transaction(DataSource dataSource, SqlWork<T> work) {
        try (Connection connection = dataSource.getConnection()) {
            boolean originalAutoCommit = connection.getAutoCommit();
            connection.setAutoCommit(false);
            try {
                T result = work.execute(connection);
                connection.commit();
                return result;
            } catch (Exception failure) {
                rollback(connection, failure);
                throw propagate(failure);
            } finally {
                connection.setAutoCommit(originalAutoCommit);
            }
        } catch (SQLException failure) {
            throw new IllegalStateException("PostgreSQL shuffle transaction failed", failure);
        }
    }

    public static <T> T read(DataSource dataSource, SqlWork<T> work) {
        try (Connection connection = dataSource.getConnection()) {
            return work.execute(connection);
        } catch (Exception failure) {
            throw propagate(failure);
        }
    }

    private static void rollback(Connection connection, Exception failure) {
        try {
            connection.rollback();
        } catch (SQLException rollbackFailure) {
            failure.addSuppressed(rollbackFailure);
        }
    }

    private static RuntimeException propagate(Exception failure) {
        if (failure instanceof RuntimeException runtime) {
            return runtime;
        }
        return new IllegalStateException("PostgreSQL shuffle operation failed", failure);
    }

    @FunctionalInterface
    public interface SqlWork<T> {
        T execute(Connection connection) throws Exception;
    }
}
