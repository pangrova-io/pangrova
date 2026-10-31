package org.pangrova.streaming.store.jdbc.watermark;

import org.pangrova.common.concurrent.ThreadSafe;
import org.pangrova.streaming.api.watermark.WatermarkStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.Optional;

/**
 * JDBC-backed {@link WatermarkStore} for persistent watermark state (F-STREAM-03).
 *
 * <p>Persists per-table watermark positions to a shared JDBC state store. On startup,
 * the {@code WatermarkTracker} loads persisted watermarks to avoid resetting to epoch,
 * which would cause late data to be incorrectly accepted after restarts.</p>
 *
 * <p>Concurrency strategy: relies on database-level row locking (MERGE/UPSERT).</p>
 */
@ThreadSafe
public final class JdbcWatermarkStore implements WatermarkStore {

    private static final Logger LOG = LoggerFactory.getLogger(JdbcWatermarkStore.class);

    private static final String CREATE_TABLE = """
            CREATE TABLE IF NOT EXISTS cdc_watermarks (
                table_name VARCHAR(512) PRIMARY KEY,
                watermark_ts TIMESTAMP NOT NULL,
                updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
            )""";

    private static final String UPSERT = """
            MERGE INTO cdc_watermarks (table_name, watermark_ts, updated_at)
            KEY (table_name) VALUES (?, ?, CURRENT_TIMESTAMP)""";

    private static final String SELECT = "SELECT watermark_ts FROM cdc_watermarks WHERE table_name = ?";

    private final DataSource dataSource;

    public JdbcWatermarkStore(DataSource dataSource) {
        this.dataSource = dataSource;
        initSchema();
    }

    private void initSchema() {
        try (Connection conn = dataSource.getConnection();
             Statement statement = conn.createStatement()) {
            statement.execute(CREATE_TABLE);
            LOG.info("event=jdbc_watermark_store_initialized");
        } catch (SQLException e) {
            LOG.warn("event=jdbc_watermark_store_init_failed error={}", e.getMessage(), e);
        }
    }

    @Override
    public void save(String tableName, Instant watermark) {
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(UPSERT)) {
            ps.setString(1, tableName);
            ps.setTimestamp(2, Timestamp.from(watermark));
            ps.executeUpdate();
        } catch (SQLException e) {
            LOG.error("event=save_watermark_failed table={} error={}", tableName, e.getMessage(), e);
            throw new RuntimeException("Failed to persist watermark for " + tableName, e);
        }
    }

    @Override
    public Optional<Instant> load(String tableName) {
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(SELECT)) {
            ps.setString(1, tableName);
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    Timestamp ts = rs.getTimestamp("watermark_ts");
                    return Optional.of(ts.toInstant());
                }
            }
        } catch (SQLException e) {
            LOG.error("event=load_watermark_failed table={} error={}", tableName, e.getMessage(), e);
        }
        return Optional.empty();
    }
}
