package org.pangrova.streaming.cdc;

import org.pangrova.common.concurrent.ThreadSafe;
import org.pangrova.spi.streaming.CdcRegistration;
import org.pangrova.spi.streaming.CdcRegistrationStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

/**
 * JDBC-backed {@link CdcRegistrationStore} for persistent CDC pipeline registration (F-STREAM-02).
 *
 * <p>Persists CDC registrations to a shared JDBC state store, ensuring pipeline
 * configurations survive coordinator restarts and are visible across all coordinators.</p>
 */
@ThreadSafe
public final class JdbcCdcRegistrationStore implements CdcRegistrationStore {

    private static final Logger LOG = LoggerFactory.getLogger(JdbcCdcRegistrationStore.class);

    private static final String CREATE_TABLE = """
            CREATE TABLE IF NOT EXISTS cdc_registrations (
                stream_id VARCHAR(255) PRIMARY KEY,
                consumer_id VARCHAR(255) NOT NULL,
                tenant_id VARCHAR(255),
                state VARCHAR(32) NOT NULL,
                max_concurrent_writes INT NOT NULL DEFAULT 4,
                max_pending_bytes BIGINT NOT NULL DEFAULT 67108864,
                last_heartbeat TIMESTAMP,
                created_at TIMESTAMP NOT NULL
            )""";

    private static final String UPSERT = """
            MERGE INTO cdc_registrations (stream_id, consumer_id, tenant_id, state,
            max_concurrent_writes, max_pending_bytes, last_heartbeat, created_at)
            KEY (stream_id) VALUES (?, ?, ?, ?, ?, ?, ?, ?)""";

    private static final String DELETE = "DELETE FROM cdc_registrations WHERE stream_id = ?";
    private static final String SELECT_ONE = "SELECT * FROM cdc_registrations WHERE stream_id = ?";
    private static final String SELECT_ALL = "SELECT * FROM cdc_registrations";

    private final DataSource dataSource;

    public JdbcCdcRegistrationStore(DataSource dataSource) {
        this.dataSource = dataSource;
        initSchema();
    }

    private void initSchema() {
        try (Connection conn = dataSource.getConnection()) {
            conn.createStatement().execute(CREATE_TABLE);
            LOG.info("event=jdbc_cdc_registration_store_initialized");
        } catch (SQLException e) {
            LOG.warn("event=jdbc_cdc_registration_store_init_failed error={}", e.getMessage(), e);
        }
    }

    @Override
    public void register(CdcRegistration registration) {
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(UPSERT)) {
            ps.setString(1, registration.streamId());
            ps.setString(2, registration.consumerId());
            ps.setString(3, registration.tenantId());
            ps.setString(4, registration.state().name());
            ps.setInt(5, registration.maxConcurrentWrites());
            ps.setLong(6, registration.maxPendingBytes());
            ps.setTimestamp(7, registration.lastHeartbeat() != null
                    ? Timestamp.from(registration.lastHeartbeat()) : null);
            ps.setTimestamp(8, Timestamp.from(registration.createdAt()));
            ps.executeUpdate();
        } catch (SQLException e) {
            LOG.error("event=register_cdc_failed streamId={} error={}",
                    registration.streamId(), e.getMessage(), e);
            throw new RuntimeException("Failed to persist CDC registration", e);
        }
    }

    @Override
    public boolean unregister(String streamId) {
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(DELETE)) {
            ps.setString(1, streamId);
            return ps.executeUpdate() > 0;
        } catch (SQLException e) {
            LOG.error("event=unregister_cdc_failed streamId={} error={}", streamId, e.getMessage(), e);
            return false;
        }
    }

    @Override
    public Optional<CdcRegistration> getRegistration(String streamId) {
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(SELECT_ONE)) {
            ps.setString(1, streamId);
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    return Optional.of(mapRegistration(rs));
                }
            }
        } catch (SQLException e) {
            LOG.error("event=get_registration_failed streamId={} error={}", streamId, e.getMessage(), e);
        }
        return Optional.empty();
    }

    @Override
    public List<CdcRegistration> listRegistrations() {
        List<CdcRegistration> result = new ArrayList<>();
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(SELECT_ALL);
             ResultSet rs = ps.executeQuery()) {
            while (rs.next()) {
                result.add(mapRegistration(rs));
            }
        } catch (SQLException e) {
            LOG.error("event=list_registrations_failed error={}", e.getMessage(), e);
        }
        return Collections.unmodifiableList(result);
    }

    private static CdcRegistration mapRegistration(ResultSet rs) throws SQLException {
        Timestamp heartbeat = rs.getTimestamp("last_heartbeat");
        return new CdcRegistration(
                rs.getString("stream_id"),
                rs.getString("consumer_id"),
                rs.getString("tenant_id"),
                CdcRegistration.CdcStreamState.valueOf(rs.getString("state")),
                rs.getInt("max_concurrent_writes"),
                rs.getLong("max_pending_bytes"),
                heartbeat != null ? heartbeat.toInstant() : null,
                rs.getTimestamp("created_at").toInstant()
        );
    }
}
