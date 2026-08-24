package org.pangrova.metadata.dynamictable;

import org.pangrova.common.concurrent.ThreadSafe;
import org.pangrova.metadata.exception.MetastoreException;
import org.pangrova.metadata.store.JdbcSupport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * JDBC-backed implementation of {@link RefreshHistoryStore}.
 *
 * <p>Persists refresh history to the {@code dynamic_table_refresh_history}
 * table created by migration V9.</p>
 */
@ThreadSafe
public final class JdbcRefreshHistoryStore implements RefreshHistoryStore {

    private static final Logger LOG = LoggerFactory.getLogger(JdbcRefreshHistoryStore.class);

    private final DataSource dataSource;

    public JdbcRefreshHistoryStore(DataSource dataSource) {
        this.dataSource = Objects.requireNonNull(dataSource, "dataSource must not be null");
    }

    @Override
    public void record(RefreshHistoryEntry entry) {
        String sql = "INSERT INTO dynamic_table_refresh_history "
                + "(refresh_id, dt_name, tenant_id, trigger_type, start_time, end_time, "
                + "status, processed_rows, bytes_written, from_checkpoint, to_checkpoint, "
                + "error_message) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)";
        executeUpdate(sql, ps -> bindEntry(ps, entry));
        LOG.debug("action=refresh_history_record dtName={} refreshId={} status={}",
                entry.dtName(), entry.refreshId(), entry.status());
    }

    @Override
    public List<RefreshHistoryEntry> query(String dtName, Instant from, Instant to, int limit) {
        return query(dtName, from, to, limit, 0);
    }

    @Override
    public List<RefreshHistoryEntry> query(String dtName, Instant from, Instant to,
                                           int limit, int offset) {
        String sql = "SELECT * FROM dynamic_table_refresh_history "
                + "WHERE dt_name = ? AND start_time >= ? AND start_time <= ? "
                + "ORDER BY start_time DESC LIMIT ? OFFSET ?";
        return executeQuery(sql, ps -> {
            ps.setString(1, dtName);
            ps.setTimestamp(2, Timestamp.from(from));
            ps.setTimestamp(3, Timestamp.from(to));
            ps.setInt(4, limit);
            ps.setInt(5, offset);
        }, this::mapRows);
    }

    @Override
    public void purge(Instant olderThan, int maxEntries) {
        String sql = "DELETE FROM dynamic_table_refresh_history "
                + "WHERE start_time < ? AND refresh_id NOT IN ("
                + "  SELECT refresh_id FROM ("
                + "    SELECT refresh_id, dt_name, "
                + "    ROW_NUMBER() OVER (PARTITION BY dt_name ORDER BY start_time DESC) AS rn "
                + "    FROM dynamic_table_refresh_history"
                + "  ) ranked WHERE rn <= ?"
                + ")";
        executeUpdate(sql, ps -> {
            ps.setTimestamp(1, Timestamp.from(olderThan));
            ps.setInt(2, maxEntries);
        });
        LOG.debug("action=refresh_history_purge olderThan={} retainCount={}", olderThan, maxEntries);
    }

    // ── Row mapping ─────────────────────────────────────────────────

    private static void bindEntry(PreparedStatement ps, RefreshHistoryEntry e) throws SQLException {
        int i = 1;
        ps.setString(i++, e.refreshId());
        ps.setString(i++, e.dtName());
        ps.setString(i++, e.tenantId());
        ps.setString(i++, e.triggerType().name());
        ps.setTimestamp(i++, Timestamp.from(e.startTime()));
        ps.setTimestamp(i++, Timestamp.from(e.endTime()));
        ps.setString(i++, e.status().name());
        ps.setLong(i++, e.processedRows());
        ps.setLong(i++, e.bytesWritten());
        ps.setString(i++, e.fromCheckpoint());
        ps.setString(i++, e.toCheckpoint());
        if (e.errorMessage() != null) {
            ps.setString(i, e.errorMessage());
        } else {
            ps.setNull(i, Types.VARCHAR);
        }
    }

    private RefreshHistoryEntry mapRow(ResultSet rs) throws SQLException {
        return new RefreshHistoryEntry(
                rs.getString("refresh_id"),
                rs.getString("dt_name"),
                rs.getString("tenant_id"),
                TriggerType.valueOf(rs.getString("trigger_type")),
                rs.getTimestamp("start_time").toInstant(),
                rs.getTimestamp("end_time").toInstant(),
                RefreshStatus.valueOf(rs.getString("status")),
                rs.getLong("processed_rows"),
                rs.getLong("bytes_written"),
                rs.getString("from_checkpoint"),
                rs.getString("to_checkpoint"),
                rs.getString("error_message"));
    }

    private List<RefreshHistoryEntry> mapRows(ResultSet rs) throws SQLException {
        List<RefreshHistoryEntry> result = new ArrayList<>();
        while (rs.next()) {
            result.add(mapRow(rs));
        }
        return List.copyOf(result);
    }

    // ── JDBC helpers ────────────────────────────────────────────────

    private <T> T executeQuery(String sql, JdbcSupport.SqlConsumer binder,
            JdbcSupport.SqlFunction<ResultSet, T> extractor) {
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {
            binder.accept(ps);
            try (ResultSet rs = ps.executeQuery()) {
                return extractor.apply(rs);
            }
        } catch (SQLException e) {
            throw new MetastoreException("Query failed: " + sql, e);
        }
    }

    private void executeUpdate(String sql, JdbcSupport.SqlConsumer binder) {
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {
            binder.accept(ps);
            ps.executeUpdate();
        } catch (SQLException e) {
            throw new MetastoreException("Update failed: " + sql, e);
        }
    }

}
