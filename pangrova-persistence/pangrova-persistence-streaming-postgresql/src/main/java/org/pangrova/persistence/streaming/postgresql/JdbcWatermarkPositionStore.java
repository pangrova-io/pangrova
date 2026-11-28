package org.pangrova.persistence.streaming.postgresql;

import org.pangrova.common.concurrency.ThreadSafe;
import org.pangrova.streaming.api.watermark.WatermarkStore;
import org.pangrova.streaming.api.exception.StreamingStoreException;

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
 * to the {@code watermark_positions} table.
 *
 * <p>Uses PostgreSQL ON CONFLICT upsert for atomic persistence.
 * No in-process mutable state — relies on database row-level locking.</p>
 */
@ThreadSafe
public final class JdbcWatermarkPositionStore implements WatermarkStore {

    private static final String UPSERT_SQL =
            "INSERT INTO watermark_positions (source_id, position, updated_at) "
                    + "VALUES (?, ?, ?) ON CONFLICT (source_id) DO UPDATE SET "
                    + "position=GREATEST(watermark_positions.position, EXCLUDED.position), "
                    + "updated_at=CASE WHEN EXCLUDED.position > watermark_positions.position "
                    + "THEN EXCLUDED.updated_at ELSE watermark_positions.updated_at END RETURNING position";

    private static final String SELECT_SQL =
            "SELECT position FROM watermark_positions WHERE source_id = ?";

    private final DataSource dataSource;

    public JdbcWatermarkPositionStore(DataSource dataSource) {
        this.dataSource = Objects.requireNonNull(dataSource, "dataSource must not be null");
    }

    @Override
    public Instant advance(String tableName, Instant watermark) {
        Timestamp now = Timestamp.from(Instant.now());
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(UPSERT_SQL)) {
            ps.setString(1, tableName);
            ps.setTimestamp(2, Timestamp.from(watermark));
            ps.setTimestamp(3, now);
            try (ResultSet result = ps.executeQuery()) {
                if (!result.next()) throw new SQLException("Advancing a watermark returned no position");
                return result.getTimestamp("position").toInstant();
            }
        } catch (SQLException e) {
            throw new StreamingStoreException(
                    "Failed to save watermark for source=" + tableName, e);
        }
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
