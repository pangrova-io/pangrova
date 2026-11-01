package org.pangrova.streaming.jdbc;

import org.pangrova.common.concurrent.ThreadSafe;
import org.pangrova.spi.streaming.exception.StreamingStoreException;
import org.pangrova.spi.streaming.store.WatermarkStore;
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
 * JDBC-backed {@link WatermarkStore} persisting watermark positions
 * to the {@code watermark_positions} table (migration V12).
 *
 * <p>Uses PostgreSQL ON CONFLICT upsert for atomic persistence.
 * No in-process mutable state — relies on database row-level locking.</p>
 */
@ThreadSafe
public final class JdbcWatermarkStore implements WatermarkStore {

    private static final Logger LOG = LoggerFactory.getLogger(JdbcWatermarkStore.class);

    private static final String UPSERT_SQL =
            "INSERT INTO watermark_positions (source_id, position, updated_at) "
                    + "VALUES (?, ?, ?) ON CONFLICT (source_id) DO UPDATE SET "
                    + "position=CASE WHEN EXCLUDED.position > watermark_positions.position "
                    + "THEN EXCLUDED.position ELSE watermark_positions.position END, "
                    + "updated_at=CASE WHEN EXCLUDED.position > watermark_positions.position "
                    + "THEN EXCLUDED.updated_at ELSE watermark_positions.updated_at END";

    private static final String SELECT_SQL =
            "SELECT position FROM watermark_positions WHERE source_id = ?";

    private final DataSource dataSource;

    public JdbcWatermarkStore(DataSource dataSource) {
        this.dataSource = Objects.requireNonNull(dataSource, "dataSource must not be null");
    }

    @Override
    public void save(String tableName, Instant watermark) {
        Timestamp now = Timestamp.from(Instant.now());
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(UPSERT_SQL)) {
            ps.setString(1, tableName);
            ps.setTimestamp(2, Timestamp.from(watermark));
            ps.setTimestamp(3, now);
            ps.executeUpdate();
        } catch (SQLException e) {
            throw new StreamingStoreException(
                    "Failed to save watermark for source=" + tableName, e);
        }
        LOG.debug("action=watermark_save source={} position={}", tableName, watermark);
    }

    @Override
    public Optional<Instant> load(String tableName) {
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(SELECT_SQL)) {
            ps.setString(1, tableName);
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    return Optional.of(rs.getTimestamp("position").toInstant());
                }
                return Optional.empty();
            }
        } catch (SQLException e) {
            throw new StreamingStoreException(
                    "Failed to load watermark for source=" + tableName, e);
        }
    }
}
