package org.pangrova.security.tenant.quota;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Objects;

/** PostgreSQL live query reservations, serialized by the tenant row in one transaction. */
public final class JdbcClusterConcurrencySlots implements ClusterConcurrencySlots {

    private static final String COUNTERS_DDL = "CREATE TABLE IF NOT EXISTS tenant_concurrency_slots ("
            + "tenant_id VARCHAR(255) PRIMARY KEY, active_count INT NOT NULL CHECK (active_count >= 0))";
    private static final String OWNERS_DDL = "CREATE TABLE IF NOT EXISTS tenant_concurrency_queries ("
            + "query_id VARCHAR(255) PRIMARY KEY, tenant_id VARCHAR(255) NOT NULL "
            + "REFERENCES tenant_concurrency_slots(tenant_id))";
    private final DataSource dataSource;

    /** @param dataSource shared PostgreSQL authority, whose failures never mean zero usage */
    public JdbcClusterConcurrencySlots(DataSource dataSource) {
        this.dataSource = Objects.requireNonNull(dataSource, "dataSource");
        try (var connection = dataSource.getConnection(); var statement = connection.createStatement()) {
            statement.execute(COUNTERS_DDL);
            statement.execute(OWNERS_DDL);
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
            throw new IllegalStateException("cannot initialize cluster admission authority", failure);
        }
    }

    @Override
    public boolean tryReserve(String tenantId, String queryId, int limit) {
        InMemoryClusterConcurrencySlots.requireIdentity(tenantId, queryId);
        if (limit <= 0) {
            return false;
        }
        try (var connection = dataSource.getConnection()) {
            connection.setAutoCommit(false);
            try {
                int active = lockTenant(connection, tenantId);
                var owner = queryOwner(connection, queryId);
                if (owner != null) {
                    connection.commit();
                    return owner.equals(tenantId);
                }
                if (active >= limit || !insertOwner(connection, tenantId, queryId)) {
                    connection.rollback();
                    return false;
                }
                adjustCount(connection, tenantId, 1);
                connection.commit();
                return true;
            } catch (SQLException | RuntimeException failure) {
                rollback(connection, failure);
                throw failure;
            }
        } catch (SQLException failure) {
            throw new IllegalStateException("cluster admission reservation failed tenantId=" + tenantId
                    + " queryId=" + queryId, failure);
        }
    }

    @Override
    public void release(String tenantId, String queryId) {
        InMemoryClusterConcurrencySlots.requireIdentity(tenantId, queryId);
        try (var connection = dataSource.getConnection()) {
            connection.setAutoCommit(false);
            try {
                lockTenant(connection, tenantId);
                try (var statement = connection.prepareStatement(
                        "DELETE FROM tenant_concurrency_queries WHERE tenant_id = ? AND query_id = ?")) {
                    statement.setString(1, tenantId);
                    statement.setString(2, queryId);
                    if (statement.executeUpdate() == 1) {
                        adjustCount(connection, tenantId, -1);
                    }
                }
                connection.commit();
            } catch (SQLException | RuntimeException failure) {
                rollback(connection, failure);
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

    private static int lockTenant(Connection connection, String tenantId) throws SQLException {
        try (var statement = connection.prepareStatement("INSERT INTO tenant_concurrency_slots "
                + "(tenant_id, active_count) VALUES (?, 0) ON CONFLICT (tenant_id) DO NOTHING")) {
            statement.setString(1, tenantId);
            statement.executeUpdate();
        }
        try (var statement = connection.prepareStatement(
                "SELECT active_count FROM tenant_concurrency_slots WHERE tenant_id = ? FOR UPDATE")) {
            statement.setString(1, tenantId);
            try (var result = statement.executeQuery()) {
                if (!result.next() || result.getInt(1) < 0) {
                    throw new IllegalStateException("missing or invalid tenant admission authority");
                }
                return result.getInt(1);
            }
        }
    }

    private static String queryOwner(Connection connection, String queryId) throws SQLException {
        try (var statement = connection.prepareStatement(
                "SELECT tenant_id FROM tenant_concurrency_queries WHERE query_id = ?")) {
            statement.setString(1, queryId);
            try (var result = statement.executeQuery()) {
                return result.next() ? result.getString(1) : null;
            }
        }
    }

    private static boolean insertOwner(Connection connection, String tenantId, String queryId) throws SQLException {
        try (var statement = connection.prepareStatement("INSERT INTO tenant_concurrency_queries "
                + "(query_id, tenant_id) VALUES (?, ?) ON CONFLICT (query_id) DO NOTHING")) {
            statement.setString(1, queryId);
            statement.setString(2, tenantId);
            return statement.executeUpdate() == 1;
        }
    }

    private static void adjustCount(Connection connection, String tenantId, int delta) throws SQLException {
        try (var statement = connection.prepareStatement("UPDATE tenant_concurrency_slots "
                + "SET active_count = active_count + ? WHERE tenant_id = ? "
                + "AND active_count + ? BETWEEN 0 AND 2147483647")) {
            statement.setInt(1, delta);
            statement.setString(2, tenantId);
            statement.setInt(3, delta);
            if (statement.executeUpdate() != 1) {
                throw new IllegalStateException("cluster admission ownership/count mismatch");
            }
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
