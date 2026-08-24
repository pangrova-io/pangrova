package org.pangrova.security.tenant.quota;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Objects;

/**
 * A-ADMISSION-01: JDBC-backed {@link ClusterConcurrencySlots} providing an ATOMIC, cluster-wide
 * concurrency gate shared by all coordinators. Reservation uses a conditional row update
 * ({@code active_count = active_count + 1 WHERE active_count < limit}), which the database serializes
 * per row, so concurrent reservations from multiple coordinators cannot oversubscribe the limit.
 *
 * <p>Fails CLOSED: if the shared store is unavailable, {@link #tryReserve} returns {@code false}
 * (the caller queues/rejects) rather than admitting blindly, so a store outage cannot cause oversell.
 */
public final class JdbcClusterConcurrencySlots implements ClusterConcurrencySlots {

    private static final Logger LOG = LoggerFactory.getLogger(JdbcClusterConcurrencySlots.class);

    private static final String DDL =
            "CREATE TABLE IF NOT EXISTS tenant_concurrency_slots ("
                    + "tenant_id VARCHAR(255) PRIMARY KEY, "
                    + "active_count INT NOT NULL)";
    private static final String SQL_RESERVE =
            "UPDATE tenant_concurrency_slots SET active_count = active_count + 1 "
                    + "WHERE tenant_id = ? AND active_count < ?";
    private static final String SQL_INSERT_FIRST =
            "INSERT INTO tenant_concurrency_slots (tenant_id, active_count) VALUES (?, 1)";
    private static final String SQL_EXISTS =
            "SELECT 1 FROM tenant_concurrency_slots WHERE tenant_id = ?";
    private static final String SQL_RELEASE =
            "UPDATE tenant_concurrency_slots SET active_count = active_count - 1 "
                    + "WHERE tenant_id = ? AND active_count > 0";
    private static final String SQL_ACTIVE =
            "SELECT active_count FROM tenant_concurrency_slots WHERE tenant_id = ?";

    private final DataSource dataSource;

    public JdbcClusterConcurrencySlots(DataSource dataSource) {
        this.dataSource = Objects.requireNonNull(dataSource, "dataSource must not be null");
        initSchema();
    }

    private void initSchema() {
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(DDL)) {
            ps.execute();
        } catch (SQLException e) {
            throw new IllegalStateException("failed to create tenant_concurrency_slots table", e);
        }
    }

    @Override
    public boolean tryReserve(String tenantId, int limit) {
        if (limit <= 0) {
            return false;
        }
        try (Connection conn = dataSource.getConnection()) {
            // Fast path: conditional increment on an existing row (atomic per-row in the DB).
            if (conditionalIncrement(conn, tenantId, limit)) {
                return true;
            }
            // No affected row: either the tenant row does not exist yet, or it is at the limit.
            if (rowExists(conn, tenantId)) {
                return false; // at limit
            }
            // Create the row reserving the first slot. Concurrent creators race on the PK; the loser
            // retries the conditional increment above.
            try (PreparedStatement ps = conn.prepareStatement(SQL_INSERT_FIRST)) {
                ps.setString(1, tenantId);
                ps.executeUpdate();
                return true;
            } catch (SQLException insertConflict) {
                return conditionalIncrement(conn, tenantId, limit);
            }
        } catch (SQLException e) {
            LOG.warn("event=cluster_slot_reserve_failed tenantId={} error={}", tenantId, e.getMessage());
            return false; // fail closed — never oversell on a store error
        }
    }

    private boolean conditionalIncrement(Connection conn, String tenantId, int limit) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(SQL_RESERVE)) {
            ps.setString(1, tenantId);
            ps.setInt(2, limit);
            return ps.executeUpdate() == 1;
        }
    }

    private boolean rowExists(Connection conn, String tenantId) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(SQL_EXISTS)) {
            ps.setString(1, tenantId);
            try (ResultSet rs = ps.executeQuery()) {
                return rs.next();
            }
        }
    }

    @Override
    public void release(String tenantId) {
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(SQL_RELEASE)) {
            ps.setString(1, tenantId);
            ps.executeUpdate();
        } catch (SQLException e) {
            LOG.warn("event=cluster_slot_release_failed tenantId={} error={}", tenantId, e.getMessage());
        }
    }

    @Override
    public int active(String tenantId) {
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(SQL_ACTIVE)) {
            ps.setString(1, tenantId);
            try (ResultSet rs = ps.executeQuery()) {
                return rs.next() ? Math.max(0, rs.getInt(1)) : 0;
            }
        } catch (SQLException e) {
            LOG.warn("event=cluster_slot_active_failed tenantId={} error={}", tenantId, e.getMessage());
            return 0;
        }
    }
}
