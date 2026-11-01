package org.pangrova.indexing.store.jdbc;

import org.pangrova.security.authz.DeferredAuthorizationGuard;
import org.pangrova.indexing.control.IndexStoreException;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLIntegrityConstraintViolationException;
import java.util.Collections;

/**
 * Shared JDBC infrastructure for index metadata sub-stores.
 *
 * <p>Provides connection management, query/update execution, and
 * common SQL helpers. Each sub-store extends this class and focuses
 * on a single responsibility domain.</p>
 */
abstract class JdbcStoreSupport {

    static final int BATCH_PARTITION_SIZE = 1000;

    private final DataSource dataSource;
    private final DeferredAuthorizationGuard authorizationGuard;

    JdbcStoreSupport(DataSource dataSource, DeferredAuthorizationGuard authorizationGuard) {
        this.dataSource = dataSource;
        this.authorizationGuard = java.util.Objects.requireNonNull(
                authorizationGuard, "authorizationGuard must not be null");
    }

    DataSource dataSource() {
        return dataSource;
    }

    DeferredAuthorizationGuard authorizationGuard() {
        return authorizationGuard;
    }

    <T> T executeQuery(String sql, SqlConsumer binder, SqlFunction<ResultSet, T> extractor) {
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {
            binder.accept(ps);
            try (ResultSet rs = ps.executeQuery()) {
                return extractor.apply(rs);
            }
        } catch (SQLException e) {
            throw new IndexStoreException("Query failed: " + sql, e);
        }
    }

    void executeUpdate(String sql, SqlConsumer binder) {
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {
            binder.accept(ps);
            ps.executeUpdate();
        } catch (SQLException e) {
            throw new IndexStoreException("Update failed: " + sql, e);
        }
    }

    int executeUpdateCount(String sql, SqlConsumer binder) {
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {
            binder.accept(ps);
            return ps.executeUpdate();
        } catch (SQLException e) {
            throw new IndexStoreException("Update failed: " + sql, e);
        }
    }

    static boolean isUniqueConstraintViolation(IndexStoreException e) {
        Throwable cause = e.getCause();
        if (cause instanceof SQLIntegrityConstraintViolationException) {
            return true;
        }
        if (cause instanceof SQLException sqlEx) {
            String sqlState = sqlEx.getSQLState();
            return "23505".equals(sqlState) || "23000".equals(sqlState);
        }
        return false;
    }

    @FunctionalInterface
    interface SqlConsumer {
        void accept(PreparedStatement ps) throws SQLException;
    }

    @FunctionalInterface
    interface SqlFunction<T, R> {
        R apply(T input) throws SQLException;
    }

    static String placeholders(int count) {
        return String.join(",", Collections.nCopies(count, "?"));
    }
}
