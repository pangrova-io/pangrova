package org.pangrova.shuffle.server.persistence;

import static org.pangrova.persistence.jdbc.support.JdbcSupport.rollbackAfterFailure;

import java.sql.Connection;
import java.sql.SQLException;

import javax.sql.DataSource;

/** Executes PostgreSQL work with explicit commit and rollback semantics. */
public final class JdbcTransactions {

    private JdbcTransactions() {}

    /** Runs one unit of work in an explicit transaction, rolling back on failure. */
    public static <T> T transaction(DataSource dataSource, SqlWork<T> work) {
        try (Connection connection = dataSource.getConnection()) {
            connection.setAutoCommit(false);
            try {
                T result = work.execute(connection);
                connection.commit();
                return result;
            } catch (Exception failure) {
                rollbackAfterFailure(connection, failure);
                throw propagate(failure);
            } catch (Error failure) {
                rollbackAfterFailure(connection, failure);
                throw failure;
            }
        } catch (SQLException failure) {
            throw new IllegalStateException("PostgreSQL shuffle transaction failed", failure);
        }
    }

    /** Runs one read operation using a managed connection. */
    public static <T> T read(DataSource dataSource, SqlWork<T> work) {
        try (Connection connection = dataSource.getConnection()) {
            return work.execute(connection);
        } catch (Exception failure) {
            throw propagate(failure);
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
