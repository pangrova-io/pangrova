package org.pangrova.metadata.core.checkpoint;

import org.pangrova.common.concurrent.ThreadSafe;
import org.pangrova.metadata.api.error.MetastoreException;
import org.pangrova.spi.resilience.CheckpointStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.Objects;
import java.util.Optional;

/**
 * JDBC-backed {@link CheckpointStore} persisting task checkpoint state
 * to the {@code checkpoints} table.
 *
 * <p>Uses PostgreSQL ON CONFLICT upsert for atomic persistence.
 * No in-process mutable state — relies on database row-level locking.</p>
 */
@ThreadSafe
public final class JdbcCheckpointStore implements CheckpointStore {

    private static final Logger LOG = LoggerFactory.getLogger(JdbcCheckpointStore.class);

    private static final String UPSERT_SQL =
            "INSERT INTO checkpoints (component_id, state, saved_at) "
                    + "VALUES (?, ?, ?) ON CONFLICT (component_id) DO UPDATE SET "
                    + "state=EXCLUDED.state, saved_at=EXCLUDED.saved_at";

    private static final String SELECT_SQL =
            "SELECT state FROM checkpoints WHERE component_id = ?";

    private static final String EXISTS_SQL =
            "SELECT 1 FROM checkpoints WHERE component_id = ?";

    private static final String DELETE_SQL =
            "DELETE FROM checkpoints WHERE component_id = ?";

    private final DataSource dataSource;

    public JdbcCheckpointStore(DataSource dataSource) {
        this.dataSource = Objects.requireNonNull(dataSource, "dataSource must not be null");
    }

    @Override
    public void saveCheckpoint(String componentId, byte[] state) {
        Timestamp now = Timestamp.from(Instant.now());
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(UPSERT_SQL)) {
            ps.setString(1, componentId);
            ps.setBytes(2, state);
            ps.setTimestamp(3, now);
            ps.executeUpdate();
        } catch (SQLException e) {
            throw new MetastoreException(
                    "Failed to save checkpoint for component=" + componentId, e);
        }
        LOG.debug("action=checkpoint_save componentId={}", componentId);
    }

    @Override
    public Optional<byte[]> loadCheckpoint(String componentId) {
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(SELECT_SQL)) {
            ps.setString(1, componentId);
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    return Optional.of(rs.getBytes("state"));
                }
                return Optional.empty();
            }
        } catch (SQLException e) {
            throw new MetastoreException(
                    "Failed to load checkpoint for component=" + componentId, e);
        }
    }

    @Override
    public boolean hasCheckpoint(String componentId) {
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(EXISTS_SQL)) {
            ps.setString(1, componentId);
            try (ResultSet rs = ps.executeQuery()) {
                return rs.next();
            }
        } catch (SQLException e) {
            throw new MetastoreException(
                    "Failed to check checkpoint for component=" + componentId, e);
        }
    }

    @Override
    public void clearCheckpoint(String componentId) {
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(DELETE_SQL)) {
            ps.setString(1, componentId);
            ps.executeUpdate();
        } catch (SQLException e) {
            throw new MetastoreException(
                    "Failed to clear checkpoint for component=" + componentId, e);
        }
        LOG.debug("action=checkpoint_clear componentId={}", componentId);
    }
}
