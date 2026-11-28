package org.pangrova.persistence.streaming.postgresql;

import org.pangrova.common.concurrency.ThreadSafe;
import org.pangrova.streaming.api.cdc.CdcRegistration;
import org.pangrova.streaming.api.enums.CdcStreamState;
import org.pangrova.streaming.api.cdc.CdcRegistrationStore;
import org.pangrova.streaming.api.exception.StreamingStoreException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import javax.sql.DataSource;

/**
 * JDBC-backed {@link CdcRegistrationStore} that persists CDC stream registrations
 * to a relational database. Uses PostgreSQL {@code INSERT ... ON CONFLICT} for upsert semantics.
 *
 * <p>Thread-safe: each operation acquires its own connection from the pool.</p>
 */
@ThreadSafe
public final class JdbcCdcRegistrationStore implements CdcRegistrationStore {

    private static final Logger LOG = LoggerFactory.getLogger(JdbcCdcRegistrationStore.class);

    // ── SQL Constants ────────────────────────────────────────────────

    private static final String SQL_MERGE = """
            INSERT INTO cdc_registrations (stream_id, consumer_id, tenant_id, state,
                max_concurrent_writes, max_pending_bytes, last_heartbeat, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT (stream_id) DO UPDATE SET
                consumer_id=EXCLUDED.consumer_id, tenant_id=EXCLUDED.tenant_id,
                state=EXCLUDED.state, max_concurrent_writes=EXCLUDED.max_concurrent_writes,
                max_pending_bytes=EXCLUDED.max_pending_bytes,
                last_heartbeat=EXCLUDED.last_heartbeat""";

    private static final String SQL_DELETE =
            "DELETE FROM cdc_registrations WHERE stream_id = ?";

    private static final String SQL_GET =
            "SELECT * FROM cdc_registrations WHERE stream_id = ?";

    private static final String SQL_LIST =
            "SELECT * FROM cdc_registrations ORDER BY created_at";

    // ── Fields ───────────────────────────────────────────────────────

    private final DataSource dataSource;

    public JdbcCdcRegistrationStore(DataSource dataSource) {
        this.dataSource = Objects.requireNonNull(dataSource, "dataSource must not be null");
    }

    // ── CdcRegistrationStore Implementation ─────────────────────────

    @Override
    public void register(CdcRegistration registration) {
        Objects.requireNonNull(registration, "registration must not be null");
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(SQL_MERGE)) {
            bindRegistration(ps, registration);
            ps.executeUpdate();
            LOG.info("event=cdc_registration_saved streamId={} consumerId={}",
                    registration.streamId(), registration.consumerId());
        } catch (SQLException e) {
            throw new StreamingStoreException(
                    "Failed to save CDC registration streamId=" + registration.streamId(), e);
        }
    }

    @Override
    public boolean unregister(String streamId) {
        Objects.requireNonNull(streamId, "streamId must not be null");
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(SQL_DELETE)) {
            ps.setString(1, streamId);
            int deleted = ps.executeUpdate();
            if (deleted > 0) {
                LOG.info("event=cdc_registration_removed streamId={}", streamId);
            }
            return deleted > 0;
        } catch (SQLException e) {
            throw new StreamingStoreException(
                    "Failed to unregister CDC stream streamId=" + streamId, e);
        }
    }

    @Override
    public Optional<CdcRegistration> getRegistration(String streamId) {
        Objects.requireNonNull(streamId, "streamId must not be null");
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(SQL_GET)) {
            ps.setString(1, streamId);
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    return Optional.of(mapRegistration(rs));
                }
                return Optional.empty();
            }
        } catch (SQLException e) {
            throw new StreamingStoreException(
                    "Failed to get CDC registration streamId=" + streamId, e);
        }
    }

    @Override
    public List<CdcRegistration> listRegistrations() {
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(SQL_LIST)) {
            try (ResultSet rs = ps.executeQuery()) {
                List<CdcRegistration> result = new ArrayList<>();
                while (rs.next()) {
                    result.add(mapRegistration(rs));
                }
                return Collections.unmodifiableList(result);
            }
        } catch (SQLException e) {
            throw new StreamingStoreException("Failed to list CDC registrations", e);
        }
    }

    // ── Private Helpers ──────────────────────────────────────────────

    private static void bindRegistration(PreparedStatement ps, CdcRegistration reg)
            throws SQLException {
        ps.setString(1, reg.streamId());
        ps.setString(2, reg.consumerId());
        ps.setString(3, reg.tenantId());
        ps.setString(4, reg.state().name());
        ps.setInt(5, reg.maxConcurrentWrites());
        ps.setLong(6, reg.maxPendingBytes());
        ps.setTimestamp(7, Timestamp.from(
                reg.lastHeartbeat() != null ? reg.lastHeartbeat() : Instant.now()));
        ps.setTimestamp(8, Timestamp.from(
                reg.createdAt() != null ? reg.createdAt() : Instant.now()));
    }

    private static CdcRegistration mapRegistration(ResultSet rs) throws SQLException {
        Timestamp lastHeartbeat = rs.getTimestamp("last_heartbeat");
        Timestamp createdAt = rs.getTimestamp("created_at");
        return new CdcRegistration(
                rs.getString("stream_id"),
                rs.getString("consumer_id"),
                rs.getString("tenant_id"),
                CdcStreamState.valueOf(rs.getString("state")),
                rs.getInt("max_concurrent_writes"),
                rs.getLong("max_pending_bytes"),
                lastHeartbeat != null ? lastHeartbeat.toInstant() : null,
                createdAt != null ? createdAt.toInstant() : null);
    }
}
