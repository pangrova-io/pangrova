package org.pangrova.coordinator.coordination;

import org.pangrova.spi.coordination.CoordinatorRegistry;
import org.pangrova.spi.coordination.CoordinatorRole;
import org.pangrova.spi.coordination.CoordinatorSnapshot;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * JDBC-backed coordinator registry using a {@code coordinator_registry} table.
 *
 * <p>Persists coordinator topology to the metadata database so that
 * clients and peer coordinators can discover the current leader endpoint
 * without hardcoded addresses. After failover, the new leader's role
 * update is immediately visible to all participants.</p>
 *
 * <p>Follows the same patterns as {@link JdbcWorkerRegistry}:
 * static SQL constants, DataSource constructor injection, SLF4J
 * structured logging, try-with-resources for all JDBC resources,
 * and SQLException wrapping into RuntimeException.</p>
 */
public final class JdbcCoordinatorRegistry implements CoordinatorRegistry {

    private static final Logger LOG = LoggerFactory.getLogger(JdbcCoordinatorRegistry.class);

    private static final long DEFAULT_STALE_THRESHOLD_MS = 30_000L;

    private static final String SQL_REGISTER =
            "INSERT INTO coordinator_registry "
                    + "(coordinator_id, host, http_port, role, epoch, last_heartbeat_ms, registered_at_ms) "
                    + "VALUES (?, ?, ?, ?, ?, ?, ?) "
                    + "ON CONFLICT (coordinator_id) DO UPDATE SET "
                    + "host=EXCLUDED.host, http_port=EXCLUDED.http_port, role=EXCLUDED.role, "
                    + "epoch=EXCLUDED.epoch, last_heartbeat_ms=EXCLUDED.last_heartbeat_ms, "
                    + "registered_at_ms=EXCLUDED.registered_at_ms";

    private static final String SQL_HEARTBEAT =
            "UPDATE coordinator_registry SET last_heartbeat_ms=? WHERE coordinator_id=?";

    private static final String SQL_UNREGISTER =
            "DELETE FROM coordinator_registry WHERE coordinator_id=?";

    private static final String SQL_UPDATE_ROLE =
            "UPDATE coordinator_registry SET role=? WHERE coordinator_id=?";

    private static final String SQL_GET_LEADER =
            "SELECT * FROM coordinator_registry WHERE role='LEADER' AND last_heartbeat_ms > ? LIMIT 1";

    private static final String SQL_GET_ALL =
            "SELECT * FROM coordinator_registry WHERE last_heartbeat_ms > ?";

    private static final String SQL_GET_COORDINATOR =
            "SELECT * FROM coordinator_registry WHERE coordinator_id=?";

    private static final String SQL_EXPIRE_STALE =
            "DELETE FROM coordinator_registry WHERE last_heartbeat_ms < ?";

    private static final String SQL_UPDATE_LAST_PROCESSED_INVALIDATION =
            "UPDATE coordinator_registry SET last_processed_invalidation_id=? WHERE coordinator_id=?";

    private final DataSource dataSource;

    public JdbcCoordinatorRegistry(DataSource dataSource) {
        this.dataSource = Objects.requireNonNull(dataSource, "dataSource must not be null");
    }

    @Override
    public void register(CoordinatorSnapshot coordinator) {
        Objects.requireNonNull(coordinator, "coordinator must not be null");
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(SQL_REGISTER)) {
            ps.setString(1, coordinator.coordinatorId());
            ps.setString(2, coordinator.host());
            ps.setInt(3, coordinator.httpPort());
            ps.setString(4, coordinator.role());
            ps.setLong(5, coordinator.epoch());
            ps.setLong(6, coordinator.lastHeartbeatMs());
            ps.setLong(7, coordinator.registeredAtMs());
            ps.executeUpdate();
            LOG.info("event=coordinator_registered coordinatorId={} host={} httpPort={}",
                    coordinator.coordinatorId(), coordinator.host(), coordinator.httpPort());
        } catch (SQLException e) {
            LOG.error("event=coordinator_register_error coordinatorId={} error={}",
                    coordinator.coordinatorId(), e.getMessage(), e);
            throw new RuntimeException("Failed to register coordinator: " + coordinator.coordinatorId(), e);
        }
    }

    @Override
    public void heartbeat(String coordinatorId) {
        Objects.requireNonNull(coordinatorId, "coordinatorId must not be null");
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(SQL_HEARTBEAT)) {
            ps.setLong(1, System.currentTimeMillis());
            ps.setString(2, coordinatorId);
            ps.executeUpdate();
            LOG.debug("event=coordinator_heartbeat coordinatorId={}", coordinatorId);
        } catch (SQLException e) {
            LOG.error("event=coordinator_heartbeat_error coordinatorId={} error={}",
                    coordinatorId, e.getMessage(), e);
            throw new RuntimeException("Failed to update heartbeat for coordinator: " + coordinatorId, e);
        }
    }

    @Override
    public void unregister(String coordinatorId) {
        Objects.requireNonNull(coordinatorId, "coordinatorId must not be null");
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(SQL_UNREGISTER)) {
            ps.setString(1, coordinatorId);
            int deleted = ps.executeUpdate();
            if (deleted > 0) {
                LOG.info("event=coordinator_unregistered coordinatorId={}", coordinatorId);
            }
        } catch (SQLException e) {
            LOG.error("event=coordinator_unregister_error coordinatorId={} error={}",
                    coordinatorId, e.getMessage(), e);
            throw new RuntimeException("Failed to unregister coordinator: " + coordinatorId, e);
        }
    }

    @Override
    public void updateRole(String coordinatorId, CoordinatorRole role) {
        Objects.requireNonNull(coordinatorId, "coordinatorId must not be null");
        Objects.requireNonNull(role, "role must not be null");
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(SQL_UPDATE_ROLE)) {
            ps.setString(1, role.name());
            ps.setString(2, coordinatorId);
            ps.executeUpdate();
            LOG.info("event=coordinator_role_updated coordinatorId={} role={}", coordinatorId, role);
        } catch (SQLException e) {
            LOG.error("event=coordinator_update_role_error coordinatorId={} role={} error={}",
                    coordinatorId, role, e.getMessage(), e);
            throw new RuntimeException("Failed to update role for coordinator: " + coordinatorId, e);
        }
    }

    @Override
    public Optional<CoordinatorSnapshot> getLeader() {
        long cutoff = System.currentTimeMillis() - DEFAULT_STALE_THRESHOLD_MS;
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(SQL_GET_LEADER)) {
            ps.setLong(1, cutoff);
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    return Optional.of(mapRow(rs));
                }
                return Optional.empty();
            }
        } catch (SQLException e) {
            LOG.error("event=coordinator_get_leader_error error={}", e.getMessage(), e);
            throw new RuntimeException("Failed to get leader coordinator", e);
        }
    }

    @Override
    public List<CoordinatorSnapshot> getAllCoordinators(Duration staleThreshold) {
        Objects.requireNonNull(staleThreshold, "staleThreshold must not be null");
        long cutoff = System.currentTimeMillis() - staleThreshold.toMillis();
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(SQL_GET_ALL)) {
            ps.setLong(1, cutoff);
            try (ResultSet rs = ps.executeQuery()) {
                List<CoordinatorSnapshot> result = new ArrayList<>();
                while (rs.next()) {
                    result.add(mapRow(rs));
                }
                return Collections.unmodifiableList(result);
            }
        } catch (SQLException e) {
            LOG.error("event=coordinator_get_all_error error={}", e.getMessage(), e);
            throw new RuntimeException("Failed to get all coordinators", e);
        }
    }

    @Override
    public Optional<CoordinatorSnapshot> getCoordinator(String coordinatorId) {
        Objects.requireNonNull(coordinatorId, "coordinatorId must not be null");
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(SQL_GET_COORDINATOR)) {
            ps.setString(1, coordinatorId);
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    return Optional.of(mapRow(rs));
                }
                return Optional.empty();
            }
        } catch (SQLException e) {
            LOG.error("event=coordinator_get_error coordinatorId={} error={}",
                    coordinatorId, e.getMessage(), e);
            throw new RuntimeException("Failed to get coordinator: " + coordinatorId, e);
        }
    }

    @Override
    public int expireStale(Duration threshold) {
        Objects.requireNonNull(threshold, "threshold must not be null");
        long cutoff = System.currentTimeMillis() - threshold.toMillis();
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(SQL_EXPIRE_STALE)) {
            ps.setLong(1, cutoff);
            int deleted = ps.executeUpdate();
            if (deleted > 0) {
                LOG.info("event=coordinator_stale_expired count={}", deleted);
            }
            return deleted;
        } catch (SQLException e) {
            LOG.error("event=coordinator_expire_stale_error error={}", e.getMessage(), e);
            throw new RuntimeException("Failed to expire stale coordinators", e);
        }
    }

    private CoordinatorSnapshot mapRow(ResultSet rs) throws SQLException {
        long lastProcessedInvalidationId = -1;
        try {
            lastProcessedInvalidationId = rs.getLong("last_processed_invalidation_id");
            if (rs.wasNull()) {
                lastProcessedInvalidationId = -1;
            }
        } catch (SQLException ignored) {
            // Column may not exist in older schemas — default to -1
        }
        return new CoordinatorSnapshot(
                rs.getString("coordinator_id"),
                rs.getString("host"),
                rs.getInt("http_port"),
                rs.getString("role"),
                rs.getLong("epoch"),
                rs.getLong("last_heartbeat_ms"),
                rs.getLong("registered_at_ms"),
                lastProcessedInvalidationId
        );
    }

    @Override
    public void updateLastProcessedInvalidationId(String coordinatorId, long lastProcessedId) {
        Objects.requireNonNull(coordinatorId, "coordinatorId must not be null");
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(SQL_UPDATE_LAST_PROCESSED_INVALIDATION)) {
            ps.setLong(1, lastProcessedId);
            ps.setString(2, coordinatorId);
            ps.executeUpdate();
        } catch (SQLException e) {
            LOG.debug("event=coordinator_update_invalidation_id_error coordinatorId={} error={}",
                    coordinatorId, e.getMessage());
            // Non-fatal: DDL propagation falls back to eventual consistency
        }
    }
}
