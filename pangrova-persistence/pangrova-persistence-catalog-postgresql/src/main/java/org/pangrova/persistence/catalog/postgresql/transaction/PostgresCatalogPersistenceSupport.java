package org.pangrova.persistence.catalog.postgresql.transaction;

import org.pangrova.common.json.CanonicalJsonMapper;

import com.fasterxml.jackson.core.JsonProcessingException;
import org.pangrova.metadata.api.enums.InvalidationType;
import org.pangrova.persistence.catalog.api.exception.MetastoreException;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.pangrova.persistence.jdbc.operation.JdbcOperations;
import org.pangrova.common.api.annotation.Internal;

/**
 * Provider-internal SQL/transaction support. Subclasses own independent domain ports;
 * callers never obtain a connection.
 */
@Internal
public abstract class PostgresCatalogPersistenceSupport {
    private static final String INSERT_INVALIDATION =
            "INSERT INTO invalidation_log (cache_key, invalidation_type, created_at, updated_at) "
                    + "VALUES (?, ?, NOW(), NOW())";
    private final DataSource dataSource;

    protected PostgresCatalogPersistenceSupport(DataSource dataSource) {
        this.dataSource = java.util.Objects.requireNonNull(dataSource, "dataSource");
    }

    protected final boolean executeMutation(String operation, String cacheKey, InvalidationType type,
            JdbcOperations.SqlFunction<Connection, Boolean> mutation) {
        try (Connection conn = dataSource.getConnection()) {
            conn.setAutoCommit(false);
            try {
                boolean changed = mutation.apply(conn);
                if (changed) {
                    executeUpdate(conn, INSERT_INVALIDATION, ps -> {
                        ps.setString(1, cacheKey);
                        ps.setString(2, type.name());
                    });
                }
                commitMutation(conn, operation, cacheKey);
                return changed;
            } catch (SQLException | RuntimeException | Error failure) {
                rollbackMutation(conn, failure);
                throw failure;
            }
        } catch (SQLException failure) {
            throw new MetastoreException(
                    "Metadata mutation failed: operation=" + operation + " cacheKey=" + cacheKey
                            + " sqlState=" + failure.getSQLState(), failure);
        }
    }

    private static void commitMutation(Connection conn, String operation, String cacheKey) {
        try {
            conn.commit();
        } catch (SQLException failure) {
            // A failed COMMIT acknowledgment cannot establish whether the database committed.
            throw new MetastoreException(
                    "Metadata mutation commit outcome unconfirmed: operation=" + operation
                            + " cacheKey=" + cacheKey + " sqlState=" + failure.getSQLState(), failure);
        }
    }

    private static void rollbackMutation(Connection conn, Throwable failure) {
        try {
            conn.rollback();
        } catch (SQLException | RuntimeException | Error rollbackFailure) {
            if (rollbackFailure != failure) {
                failure.addSuppressed(rollbackFailure);
            }
        }
    }

    protected final <T> T executeQuery(String sql, JdbcOperations.SqlConsumer binder,
            JdbcOperations.SqlFunction<ResultSet, T> extractor) {
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {
            binder.accept(ps);
            try (ResultSet rs = ps.executeQuery()) {
                return extractor.apply(rs);
            }
        } catch (SQLException e) {
            throw new MetastoreException("Query failed: " + sql, e);
        }
    }

    protected static int executeUpdate(Connection conn, String sql, JdbcOperations.SqlConsumer binder)
            throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            binder.accept(ps);
            return ps.executeUpdate();
        }
    }

    protected final String toJson(Map<String, String> value) {
        return serializeToJson(value);
    }

    protected final String toJson(Set<String> value) {
        return serializeToJson(value);
    }

    protected final String toJson(List<String> value) {
        return serializeToJson(value);
    }

    private <T> String serializeToJson(T value) {
        try {
            return CanonicalJsonMapper.writeValueAsString(value);
        } catch (JsonProcessingException e) {
            throw new MetastoreException("JSON serialization failed", e);
        }
    }
}
