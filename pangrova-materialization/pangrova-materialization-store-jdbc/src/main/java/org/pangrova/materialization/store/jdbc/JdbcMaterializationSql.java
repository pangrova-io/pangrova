package org.pangrova.materialization.store.jdbc;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import javax.sql.DataSource;
import org.pangrova.materialization.domain.MaterializationException;

/**
 * Connection, statement, and transaction plumbing shared by the materialization
 * JDBC adapters.
 *
 * <p>Failures are translated once, here, so every adapter reports the same
 * repository error code and no SQL exception escapes the materialization store.
 */
final class JdbcMaterializationSql {

    private final DataSource dataSource;

    JdbcMaterializationSql(DataSource dataSource) {
        if (dataSource == null) {
            throw new IllegalArgumentException("dataSource must not be null");
        }
        this.dataSource = dataSource;
    }

    <T> Optional<T> queryOne(String action, String sql,
            org.pangrova.common.jdbc.JdbcSupport.SqlConsumer binder,
            org.pangrova.common.jdbc.JdbcSupport.SqlFunction<ResultSet, T> mapper) {
        try (Connection connection = dataSource.getConnection();
             PreparedStatement statement = connection.prepareStatement(sql)) {
            binder.accept(statement);
            try (ResultSet result = statement.executeQuery()) {
                return result.next() ? Optional.of(mapper.apply(result)) : Optional.empty();
            }
        } catch (SQLException failure) {
            throw MaterializationException.repositoryFailure(action + " failed", failure);
        }
    }

    <T> List<T> queryList(String action, String sql,
            org.pangrova.common.jdbc.JdbcSupport.SqlConsumer binder,
            org.pangrova.common.jdbc.JdbcSupport.SqlFunction<ResultSet, T> mapper) {
        try (Connection connection = dataSource.getConnection();
             PreparedStatement statement = connection.prepareStatement(sql)) {
            binder.accept(statement);
            try (ResultSet result = statement.executeQuery()) {
                List<T> values = new ArrayList<>();
                while (result.next()) {
                    values.add(mapper.apply(result));
                }
                return values;
            }
        } catch (SQLException failure) {
            throw MaterializationException.repositoryFailure(action + " failed", failure);
        }
    }

    <T> T inTransaction(String action,
            org.pangrova.common.jdbc.JdbcSupport.SqlFunction<Connection, T> work) {
        try (Connection connection = dataSource.getConnection()) {
            connection.setAutoCommit(false);
            try {
                T result = work.apply(connection);
                connection.commit();
                return result;
            } catch (SQLException failure) {
                rollback(connection, failure);
                throw MaterializationException.repositoryFailure(action + " failed", failure);
            } catch (RuntimeException | Error failure) {
                rollback(connection, failure);
                throw failure;
            }
        } catch (SQLException failure) {
            throw MaterializationException.repositoryFailure(action + " failed", failure);
        }
    }

    private static void rollback(Connection connection, Throwable failure) {
        try {
            connection.rollback();
        } catch (SQLException rollbackFailure) {
            failure.addSuppressed(rollbackFailure);
        }
    }
}
