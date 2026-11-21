package org.pangrova.persistence.query.postgresql.audit;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Objects;
import java.util.Optional;

import javax.sql.DataSource;

import org.pangrova.query.api.audit.QueryAuditStore;
import org.pangrova.query.api.audit.QueryAuditRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Durable query-audit store over {@code query_audit_records}
 * (schema owned by {@code V7__query_audit_records.sql}).
 *
 * <p>The in-memory store is a bounded Caffeine cache: it drops entries at its
 * maximum size and loses everything on restart, so audit history could vanish
 * silently. This store keeps one row per query for the retention window.</p>
 *
 * <p>Persistence failures are logged at ERROR with an {@code audit_record_lost}
 * marker so loss is alertable, then swallowed — an audit write must never fail
 * the query it describes.</p>
 */
public final class JdbcQueryAuditStore implements QueryAuditStore {

    private static final Logger LOG = LoggerFactory.getLogger(JdbcQueryAuditStore.class);

    // Query ids are stable per execution, so a retried save must overwrite rather
    // than fail on the primary key.
    private static final String SQL_SAVE =
            "INSERT INTO query_audit_records "
                    + "(query_id, user_id, sql_text, tenant_id, stage_count, task_count, "
                    + "cpu_time_ms, wall_time_ms, peak_memory_bytes, spilled_bytes, "
                    + "input_rows, output_rows, input_bytes, output_bytes, "
                    + "shuffle_read_bytes, shuffle_write_bytes, completed_at, success) "
                    + "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) "
                    + "ON CONFLICT (query_id) DO UPDATE SET "
                    + "user_id = EXCLUDED.user_id, sql_text = EXCLUDED.sql_text, "
                    + "tenant_id = EXCLUDED.tenant_id, stage_count = EXCLUDED.stage_count, "
                    + "task_count = EXCLUDED.task_count, cpu_time_ms = EXCLUDED.cpu_time_ms, "
                    + "wall_time_ms = EXCLUDED.wall_time_ms, "
                    + "peak_memory_bytes = EXCLUDED.peak_memory_bytes, "
                    + "spilled_bytes = EXCLUDED.spilled_bytes, "
                    + "input_rows = EXCLUDED.input_rows, output_rows = EXCLUDED.output_rows, "
                    + "input_bytes = EXCLUDED.input_bytes, output_bytes = EXCLUDED.output_bytes, "
                    + "shuffle_read_bytes = EXCLUDED.shuffle_read_bytes, "
                    + "shuffle_write_bytes = EXCLUDED.shuffle_write_bytes, "
                    + "completed_at = EXCLUDED.completed_at, success = EXCLUDED.success";

    private static final String SQL_FIND_BY_ID =
            "SELECT query_id, user_id, sql_text, tenant_id, stage_count, task_count, "
                    + "cpu_time_ms, wall_time_ms, peak_memory_bytes, spilled_bytes, "
                    + "input_rows, output_rows, input_bytes, output_bytes, "
                    + "shuffle_read_bytes, shuffle_write_bytes, completed_at, success "
                    + "FROM query_audit_records WHERE query_id = ?";

    private static final String SQL_DELETE_OLDER_THAN =
            "DELETE FROM query_audit_records "
                    + "WHERE completed_at < CURRENT_TIMESTAMP - make_interval(days => ?)";

    private final DataSource dataSource;

    public JdbcQueryAuditStore(DataSource dataSource) {
        this.dataSource = Objects.requireNonNull(dataSource, "dataSource must not be null");
    }

    @Override
    public void save(QueryAuditRecord record) {
        Objects.requireNonNull(record, "record must not be null");
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(SQL_SAVE)) {
            int index = 1;
            ps.setString(index++, record.queryId());
            ps.setString(index++, record.userId());
            ps.setString(index++, record.sql());
            ps.setString(index++, record.tenantId());
            ps.setInt(index++, record.stageCount());
            ps.setInt(index++, record.taskCount());
            ps.setLong(index++, record.cpuTimeMs());
            ps.setLong(index++, record.wallTimeMs());
            ps.setLong(index++, record.peakMemoryBytes());
            ps.setLong(index++, record.spilledBytes());
            ps.setLong(index++, record.inputRows());
            ps.setLong(index++, record.outputRows());
            ps.setLong(index++, record.inputBytes());
            ps.setLong(index++, record.outputBytes());
            ps.setLong(index++, record.shuffleReadBytes());
            ps.setLong(index++, record.shuffleWriteBytes());
            ps.setTimestamp(index++, record.completedAt() == null
                    ? null : Timestamp.from(record.completedAt()));
            ps.setBoolean(index++, record.success());
            ps.executeUpdate();
        } catch (SQLException failure) {
            LOG.error("event=audit_record_lost query_id={} error={}",
                    record.queryId(), failure.getMessage(), failure);
        }
    }

    @Override
    public Optional<QueryAuditRecord> findById(String queryId) {
        Objects.requireNonNull(queryId, "queryId must not be null");
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(SQL_FIND_BY_ID)) {
            ps.setString(1, queryId);
            try (ResultSet rs = ps.executeQuery()) {
                if (!rs.next()) {
                    return Optional.empty();
                }
                Timestamp completedAt = rs.getTimestamp("completed_at");
                return Optional.of(new QueryAuditRecord(
                        rs.getString("query_id"),
                        rs.getString("user_id"),
                        rs.getString("sql_text"),
                        rs.getString("tenant_id"),
                        rs.getInt("stage_count"),
                        rs.getInt("task_count"),
                        rs.getLong("cpu_time_ms"),
                        rs.getLong("wall_time_ms"),
                        rs.getLong("peak_memory_bytes"),
                        rs.getLong("spilled_bytes"),
                        rs.getLong("input_rows"),
                        rs.getLong("output_rows"),
                        rs.getLong("input_bytes"),
                        rs.getLong("output_bytes"),
                        rs.getLong("shuffle_read_bytes"),
                        rs.getLong("shuffle_write_bytes"),
                        completedAt == null ? null : completedAt.toInstant(),
                        rs.getBoolean("success")));
            }
        } catch (SQLException failure) {
            LOG.error("event=audit_read_failed query_id={} error={}",
                    queryId, failure.getMessage(), failure);
            return Optional.empty();
        }
    }

    @Override
    public long deleteOlderThan(int retentionDays) {
        if (retentionDays <= 0) {
            throw new IllegalArgumentException("retentionDays must be positive");
        }
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(SQL_DELETE_OLDER_THAN)) {
            ps.setInt(1, retentionDays);
            return ps.executeUpdate();
        } catch (SQLException failure) {
            LOG.error("event=audit_retention_failed retention_days={} error={}",
                    retentionDays, failure.getMessage(), failure);
            return 0;
        }
    }
}
