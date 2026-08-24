package org.pangrova.metadata.streaming;

import org.pangrova.common.concurrent.ThreadSafe;
import org.pangrova.common.exception.StaleEpochException;
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
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * JDBC-backed implementation of {@link StreamStateStore}.
 *
 * <p>Persists CDC ingestion stream state to the {@code ingestion_stream_state}
 * table created by migration V6. Uses PostgreSQL ON CONFLICT upsert for
 * save operations and compare-and-swap on epoch for HA fencing.</p>
 *
 * <p>Concurrency strategy: relies on database-level row locking for
 * atomicity. No in-process mutable state. Map fields (sourceProperties,
 * streamProperties) are serialized as simple {@code key=value} lines.</p>
 */
@ThreadSafe
public final class JdbcStreamStateStore implements StreamStateStore {

    private static final Logger LOG = LoggerFactory.getLogger(JdbcStreamStateStore.class);

    private final DataSource dataSource;

    public JdbcStreamStateStore(DataSource dataSource) {
        this.dataSource = Objects.requireNonNull(dataSource, "dataSource must not be null");
    }

    @Override
    public void saveState(StreamState state) {
        String sql = "INSERT INTO ingestion_stream_state ("
                + "stream_name, tenant_id, lifecycle_state, target_table, "
                + "source_properties, source_format, stream_properties, "
                + "creator_identity, coordinator_id, epoch, schema_version, "
                + "error_message, records_consumed, records_written, "
                + "records_dead_lettered, last_offset_commit_time, "
                + "created_at, updated_at) "
                + "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?) "
                + "ON CONFLICT (stream_name) DO UPDATE SET "
                + "tenant_id=EXCLUDED.tenant_id, lifecycle_state=EXCLUDED.lifecycle_state, "
                + "target_table=EXCLUDED.target_table, source_properties=EXCLUDED.source_properties, "
                + "source_format=EXCLUDED.source_format, stream_properties=EXCLUDED.stream_properties, "
                + "creator_identity=EXCLUDED.creator_identity, coordinator_id=EXCLUDED.coordinator_id, "
                + "epoch=EXCLUDED.epoch, schema_version=EXCLUDED.schema_version, "
                + "error_message=EXCLUDED.error_message, records_consumed=EXCLUDED.records_consumed, "
                + "records_written=EXCLUDED.records_written, "
                + "records_dead_lettered=EXCLUDED.records_dead_lettered, "
                + "last_offset_commit_time=EXCLUDED.last_offset_commit_time, "
                + "updated_at=EXCLUDED.updated_at";
        executeUpdate(sql, ps -> bindState(ps, state));
        LOG.info("action=stream_state_save streamName={} state={} tenantId={}",
                state.streamName(), state.lifecycleState(), state.tenantId());
    }

    @Override
    public Optional<StreamState> loadState(String streamName) {
        String sql = "SELECT * FROM ingestion_stream_state WHERE stream_name = ?";
        return executeQuery(sql, ps -> ps.setString(1, streamName), rs -> {
            if (rs.next()) {
                return Optional.of(mapRow(rs));
            }
            return Optional.empty();
        });
    }

    @Override
    public List<StreamState> listByTenant(String tenantId) {
        String sql = "SELECT * FROM ingestion_stream_state WHERE tenant_id = ?";
        return executeQuery(sql, ps -> ps.setString(1, tenantId), this::mapRows);
    }

    @Override
    public List<StreamState> listByState(StreamLifecycleState lifecycleState) {
        String sql = "SELECT * FROM ingestion_stream_state WHERE lifecycle_state = ?";
        return executeQuery(sql,
                ps -> ps.setString(1, lifecycleState.name()),
                this::mapRows);
    }

    @Override
    public List<StreamState> listAll() {
        String sql = "SELECT * FROM ingestion_stream_state";
        return executeQuery(sql, ps -> { }, this::mapRows);
    }

    @Override
    public boolean delete(String streamName) {
        String sql = "DELETE FROM ingestion_stream_state WHERE stream_name = ?";
        int affected = executeUpdateReturning(sql, ps -> ps.setString(1, streamName));
        if (affected > 0) {
            LOG.info("action=stream_state_delete streamName={}", streamName);
            return true;
        }
        return false;
    }

    @Override
    public void saveStateIfEpoch(StreamState state, long expectedEpoch) {
        String sql = "UPDATE ingestion_stream_state SET "
                + "tenant_id = ?, lifecycle_state = ?, target_table = ?, "
                + "source_properties = ?, source_format = ?, stream_properties = ?, "
                + "creator_identity = ?, coordinator_id = ?, epoch = ?, "
                + "schema_version = ?, error_message = ?, "
                + "records_consumed = ?, records_written = ?, "
                + "records_dead_lettered = ?, last_offset_commit_time = ?, "
                + "updated_at = ? "
                + "WHERE stream_name = ? AND epoch = ?";
        int affected = executeUpdateReturning(sql,
                ps -> bindStreamCasUpdate(ps, state, expectedEpoch));
        if (affected == 0) {
            throw new StaleEpochException(state.streamName(), expectedEpoch);
        }
        LOG.info("action=stream_state_cas_save streamName={} state={} expectedEpoch={} newEpoch={}",
                state.streamName(), state.lifecycleState(), expectedEpoch, state.epoch());
    }

    private static void bindStreamCasUpdate(PreparedStatement ps, StreamState state,
                                            long expectedEpoch) throws SQLException {
        int i = 1;
        ps.setString(i++, state.tenantId());
        ps.setString(i++, state.lifecycleState().name());
        ps.setString(i++, state.targetTable());
        ps.setString(i++, serializeMap(state.sourceProperties()));
        ps.setString(i++, state.sourceFormat());
        setNullableString(ps, i++, serializeMap(state.streamProperties()));
        setNullableString(ps, i++, state.creatorIdentity());
        setNullableString(ps, i++, state.coordinatorId());
        ps.setLong(i++, state.epoch());
        ps.setInt(i++, state.schemaVersion());
        setNullableString(ps, i++, state.errorMessage());
        ps.setLong(i++, state.recordsConsumed());
        ps.setLong(i++, state.recordsWritten());
        ps.setLong(i++, state.recordsDeadLettered());
        setNullableTimestamp(ps, i++, state.lastOffsetCommitTime());
        ps.setTimestamp(i++, Timestamp.from(state.updatedAt()));
        ps.setString(i++, state.streamName());
        ps.setLong(i, expectedEpoch);
    }

    // ── Row mapping ─────────────────────────────────────────────────

    private StreamState mapRow(ResultSet rs) throws SQLException {
        Timestamp lastCommit = rs.getTimestamp("last_offset_commit_time");
        return new StreamState(
                rs.getString("stream_name"),
                rs.getString("tenant_id"),
                StreamLifecycleState.valueOf(rs.getString("lifecycle_state")),
                rs.getString("target_table"),
                deserializeMap(rs.getString("source_properties")),
                rs.getString("source_format"),
                deserializeMap(rs.getString("stream_properties")),
                rs.getString("creator_identity"),
                rs.getString("coordinator_id"),
                rs.getLong("epoch"),
                rs.getInt("schema_version"),
                rs.getString("error_message"),
                rs.getLong("records_consumed"),
                rs.getLong("records_written"),
                rs.getLong("records_dead_lettered"),
                lastCommit != null ? lastCommit.toInstant() : null,
                rs.getTimestamp("created_at").toInstant(),
                rs.getTimestamp("updated_at").toInstant());
    }

    private List<StreamState> mapRows(ResultSet rs) throws SQLException {
        List<StreamState> result = new ArrayList<>();
        while (rs.next()) {
            result.add(mapRow(rs));
        }
        return List.copyOf(result);
    }

    private static void bindState(PreparedStatement ps, StreamState s) throws SQLException {
        int i = 1;
        ps.setString(i++, s.streamName());
        ps.setString(i++, s.tenantId());
        ps.setString(i++, s.lifecycleState().name());
        ps.setString(i++, s.targetTable());
        ps.setString(i++, serializeMap(s.sourceProperties()));
        ps.setString(i++, s.sourceFormat());
        setNullableString(ps, i++, serializeMap(s.streamProperties()));
        setNullableString(ps, i++, s.creatorIdentity());
        setNullableString(ps, i++, s.coordinatorId());
        ps.setLong(i++, s.epoch());
        ps.setInt(i++, s.schemaVersion());
        setNullableString(ps, i++, s.errorMessage());
        ps.setLong(i++, s.recordsConsumed());
        ps.setLong(i++, s.recordsWritten());
        ps.setLong(i++, s.recordsDeadLettered());
        setNullableTimestamp(ps, i++, s.lastOffsetCommitTime());
        ps.setTimestamp(i++, Timestamp.from(s.createdAt()));
        ps.setTimestamp(i, Timestamp.from(s.updatedAt()));
    }

    // ── Map serialization ───────────────────────────────────────────
    // Simple key=value\n format. No external JSON library needed.
    // Keys and values are escaped: \n → \\n, = → \\=, \\ → \\\\

    static String serializeMap(Map<String, String> map) {
        if (map == null || map.isEmpty()) {
            return "";
        }
        StringBuilder sb = new StringBuilder();
        for (Map.Entry<String, String> entry : map.entrySet()) {
            if (!sb.isEmpty()) {
                sb.append('\n');
            }
            sb.append(escape(entry.getKey()))
                    .append('=')
                    .append(escape(entry.getValue()));
        }
        return sb.toString();
    }

    static Map<String, String> deserializeMap(String text) {
        if (text == null || text.isEmpty()) {
            return Map.of();
        }
        Map<String, String> map = new LinkedHashMap<>();
        for (String line : text.split("\n")) {
            int sep = findUnescapedEquals(line);
            if (sep < 0) {
                continue;
            }
            String key = unescape(line.substring(0, sep));
            String value = unescape(line.substring(sep + 1));
            map.put(key, value);
        }
        return Collections.unmodifiableMap(map);
    }

    private static String escape(String s) {
        return s.replace("\\", "\\\\")
                .replace("=", "\\=")
                .replace("\n", "\\n");
    }

    private static String unescape(String s) {
        StringBuilder sb = new StringBuilder(s.length());
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            if (c == '\\' && i + 1 < s.length()) {
                sb.append(unescapeChar(s.charAt(i + 1)));
                i++;
            } else {
                sb.append(c);
            }
        }
        return sb.toString();
    }

    private static char unescapeChar(char next) {
        return switch (next) {
            case '\\' -> '\\';
            case '=' -> '=';
            case 'n' -> '\n';
            default -> next;
        };
    }

    private static int findUnescapedEquals(String line) {
        for (int i = 0; i < line.length(); i++) {
            char c = line.charAt(i);
            if (c == '\\') {
                i++; // skip escaped char
            } else if (c == '=') {
                return i;
            }
        }
        return -1;
    }

    // ── Nullable helpers ────────────────────────────────────────────

    private static void setNullableString(PreparedStatement ps, int idx, String val)
            throws SQLException {
        if (val != null) {
            ps.setString(idx, val);
        } else {
            ps.setNull(idx, Types.VARCHAR);
        }
    }

    private static void setNullableTimestamp(PreparedStatement ps, int idx, Instant val)
            throws SQLException {
        if (val != null) {
            ps.setTimestamp(idx, Timestamp.from(val));
        } else {
            ps.setNull(idx, Types.TIMESTAMP);
        }
    }

    // ── JDBC helpers ────────────────────────────────────────────────

    private <T> T executeQuery(String sql,
                               JdbcSupport.SqlConsumer binder,
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

    private int executeUpdateReturning(String sql, JdbcSupport.SqlConsumer binder) {
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {
            binder.accept(ps);
            return ps.executeUpdate();
        } catch (SQLException e) {
            throw new MetastoreException("Update failed: " + sql, e);
        }
    }
}
