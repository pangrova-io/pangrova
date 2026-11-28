package org.pangrova.persistence.coordination.postgresql.admission;

import static org.pangrova.persistence.jdbc.support.JdbcSupport.rollbackAfterFailure;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Objects;
import org.pangrova.persistence.coordination.api.admission.ClusterConcurrencySlots;

/** PostgreSQL live query reservations, serialized by the tenant row in one transaction. */
public final class JdbcClusterConcurrencySlots implements ClusterConcurrencySlots {

    private final DataSource dataSource;

    /** @param dataSource shared PostgreSQL authority, whose failures never mean zero usage */
    public JdbcClusterConcurrencySlots(DataSource dataSource) {
        this.dataSource = Objects.requireNonNull(dataSource, "dataSource");
        verifyAuthority(dataSource);
    }

    /** Validates the persisted quota counters before enabling query admission. */
    public static void verifyAuthority(DataSource dataSource) {
        try (var connection = dataSource.getConnection(); var statement = connection.createStatement()) {
            try (var mismatch = statement.executeQuery("SELECT slots.tenant_id "
                    + "FROM tenant_concurrency_slots slots LEFT JOIN (SELECT tenant_id, COUNT(*) owners "
                    + "FROM tenant_concurrency_queries GROUP BY tenant_id) queries "
                    + "ON slots.tenant_id = queries.tenant_id "
                    + "WHERE slots.active_count <> COALESCE(queries.owners, 0) LIMIT 1")) {
                if (mismatch.next()) {
                    throw new IllegalStateException("cluster admission has an unidentified reservation; "
                            + "physical drain/reconciliation is required before activation");
                }
            }
        } catch (SQLException failure) {
            throw new IllegalStateException(
                "cannot validate cluster admission authority; initialize the canonical schema first",
                failure);
        }
    }

    @Override
    public boolean tryReserve(String tenantId, String queryId, int limit) {
        ClusterConcurrencySlots.requireIdentity(tenantId, queryId);
        if (limit <= 0) {
            return false;
        }
        try (var connection = dataSource.getConnection()) {
            connection.setAutoCommit(false);
            try {
                if (!insertWithinLimit(connection, tenantId, queryId, limit)) {
                    boolean alreadyReserved = tenantId.equals(queryOwner(connection, queryId));
                    connection.rollback();
                    return alreadyReserved;
                }
                connection.commit();
                return true;
            } catch (SQLException | RuntimeException failure) {
                rollbackAfterFailure(connection, failure);
                throw failure;
            }
        } catch (SQLException failure) {
            throw new IllegalStateException("cluster admission reservation failed tenantId=" + tenantId
                    + " queryId=" + queryId, failure);
        }
    }

    @Override
    public void release(String tenantId, String queryId) {
        ClusterConcurrencySlots.requireIdentity(tenantId, queryId);
        try (var connection = dataSource.getConnection()) {
            connection.setAutoCommit(false);
            try {
                releaseOwner(connection, tenantId, queryId);
                connection.commit();
            } catch (SQLException | RuntimeException failure) {
                rollbackAfterFailure(connection, failure);
                throw failure;
            }
        } catch (SQLException failure) {
            throw new IllegalStateException("cluster admission release failed tenantId=" + tenantId
                    + " queryId=" + queryId + "; reservation must be retried", failure);
        }
    }

    @Override
    public int active(String tenantId) {
        try (var connection = dataSource.getConnection(); var statement = connection.prepareStatement(
                "SELECT active_count FROM tenant_concurrency_slots WHERE tenant_id = ?")) {
            statement.setString(1, Objects.requireNonNull(tenantId, "tenantId"));
            try (var result = statement.executeQuery()) {
                if (!result.next()) {
                    return 0;
                }
                int count = result.getInt(1);
                if (count < 0) {
                    throw new IllegalStateException("negative cluster admission count");
                }
                return count;
            }
        } catch (SQLException failure) {
            throw new IllegalStateException("cluster admission usage unavailable tenantId=" + tenantId, failure);
        }
    }

    private static boolean insertWithinLimit(Connection connection, String tenantId, String queryId, int limit)
            throws SQLException {
        try (var statement = connection.prepareStatement("SELECT reserve_tenant_query_slot(?, ?, ?, FALSE)")) {
            statement.setString(1, tenantId);
            statement.setString(2, queryId);
            statement.setInt(3, limit);
            try (var rows = statement.executeQuery()) {
                return rows.next() && rows.getBoolean(1);
            }
        }
    }

    private static void releaseOwner(Connection connection, String tenantId, String queryId) throws SQLException {
        try (var statement = connection.prepareStatement("SELECT release_tenant_query_slot(?, ?, FALSE)")) {
            statement.setString(1, queryId);
            statement.setString(2, tenantId);
            try (var result = statement.executeQuery()) {
                if (!result.next() || !result.getBoolean(1)) {
                    throw new IllegalStateException("cluster admission ownership/count mismatch");
                }
            }
        }
    }

    private static String queryOwner(Connection connection, String queryId) throws SQLException {
        try (var statement = connection.prepareStatement(
                "SELECT tenant_id FROM tenant_concurrency_queries WHERE query_id = ? AND NOT execution_bound")) {
            statement.setString(1, queryId);
            try (var result = statement.executeQuery()) {
                return result.next() ? result.getString(1) : null;
            }
        }
    }
}
