package org.pangrova.metadata.dynamictable;

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
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * JDBC-backed implementation of {@link DynamicTableStateStore}.
 *
 * <p>Persists Dynamic Table state to the {@code dynamic_table_state} table
 * created by migration V9. Uses PostgreSQL ON CONFLICT upsert for
 * atomic persistence.</p>
 *
 * <p>Concurrency strategy: relies on database-level row locking for
 * atomicity. No in-process mutable state.</p>
 */
@ThreadSafe
public final class JdbcDynamicTableStateStore implements DynamicTableStateStore {

    private static final Logger LOG = LoggerFactory.getLogger(JdbcDynamicTableStateStore.class);

    private final DataSource dataSource;

    public JdbcDynamicTableStateStore(DataSource dataSource) {
        this.dataSource = Objects.requireNonNull(dataSource, "dataSource must not be null");
    }

    @Override
    public void saveState(DynamicTableState state) {
        String sql = "INSERT INTO dynamic_table_state (dt_name, tenant_id, lifecycle_state, "
                + "effective_refresh_mode, target_lag_ms, checkpoint, checkpoint_type, "
                + "source_schema_version, source_schema_snapshot, creator_identity, "
                + "coordinator_id, epoch, last_refresh_time, total_refresh_count, "
                + "consecutive_failures, created_at, updated_at) "
                + "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?) "
                + "ON CONFLICT (dt_name) DO UPDATE SET "
                + "tenant_id=EXCLUDED.tenant_id, lifecycle_state=EXCLUDED.lifecycle_state, "
                + "effective_refresh_mode=EXCLUDED.effective_refresh_mode, "
                + "target_lag_ms=EXCLUDED.target_lag_ms, checkpoint=EXCLUDED.checkpoint, "
                + "checkpoint_type=EXCLUDED.checkpoint_type, "
                + "source_schema_version=EXCLUDED.source_schema_version, "
                + "source_schema_snapshot=EXCLUDED.source_schema_snapshot, "
                + "creator_identity=EXCLUDED.creator_identity, "
                + "coordinator_id=EXCLUDED.coordinator_id, epoch=EXCLUDED.epoch, "
                + "last_refresh_time=EXCLUDED.last_refresh_time, "
                + "total_refresh_count=EXCLUDED.total_refresh_count, "
                + "consecutive_failures=EXCLUDED.consecutive_failures, "
                + "updated_at=EXCLUDED.updated_at";
        executeUpdate(sql, ps -> bindState(ps, state));
        LOG.info("action=dt_state_save dtName={} state={} tenantId={}",
                state.dtName(), state.state(), state.tenantId());
    }

    @Override
    public Optional<DynamicTableState> loadState(String dtName) {
        String sql = "SELECT * FROM dynamic_table_state WHERE dt_name = ?";
        return executeQuery(sql, ps -> ps.setString(1, dtName), rs -> {
            if (rs.next()) {
                return Optional.of(mapRow(rs));
            }
            return Optional.empty();
        });
    }

    @Override
    public List<DynamicTableState> listActiveByTenant(String tenantId) {
        String sql = "SELECT * FROM dynamic_table_state "
                + "WHERE tenant_id = ? AND lifecycle_state = ?";
        return executeQuery(sql, ps -> {
            ps.setString(1, tenantId);
            ps.setString(2, DynamicTableLifecycleState.ACTIVE.name());
        }, this::mapRows);
    }

    @Override
    public List<DynamicTableState> listAll() {
        String sql = "SELECT * FROM dynamic_table_state";
        return executeQuery(sql, ps -> { }, this::mapRows);
    }

    @Override
    public boolean delete(String dtName) {
        String sql = "DELETE FROM dynamic_table_state WHERE dt_name = ?";
        int affected = executeUpdateReturning(sql, ps -> ps.setString(1, dtName));
        if (affected > 0) {
            LOG.info("action=dt_state_delete dtName={}", dtName);
            return true;
        }
        return false;
    }

    @Override
    public void saveStateIfEpoch(DynamicTableState state, long expectedEpoch) {
        String sql = "UPDATE dynamic_table_state SET "
                + "tenant_id = ?, lifecycle_state = ?, effective_refresh_mode = ?, "
                + "target_lag_ms = ?, checkpoint = ?, checkpoint_type = ?, "
                + "source_schema_version = ?, source_schema_snapshot = ?, "
                + "creator_identity = ?, coordinator_id = ?, epoch = ?, "
                + "last_refresh_time = ?, total_refresh_count = ?, "
                + "consecutive_failures = ?, updated_at = ? "
                + "WHERE dt_name = ? AND epoch = ?";
        int affected = executeUpdateReturning(sql,
                ps -> bindCasUpdate(ps, state, expectedEpoch));
        if (affected == 0) {
            throw new StaleEpochException(state.dtName(), expectedEpoch);
        }
        LOG.info("action=dt_state_cas_save dtName={} state={} expectedEpoch={} newEpoch={}",
                state.dtName(), state.state(), expectedEpoch, state.epoch());
    }

    private static void bindCasUpdate(PreparedStatement ps, DynamicTableState state,
                                      long expectedEpoch) throws SQLException {
        int i = 1;
        ps.setString(i++, state.tenantId());
        ps.setString(i++, state.state().name());
        ps.setString(i++, state.effectiveRefreshMode().name());
        ps.setLong(i++, state.targetLag().toMillis());
        setNullableString(ps, i++, state.checkpoint());
        setNullableString(ps, i++, state.checkpointType());
        ps.setInt(i++, state.sourceSchemaVersion());
        setNullableString(ps, i++, state.sourceSchemaSnapshot());
        setNullableString(ps, i++, state.creatorIdentity());
        setNullableString(ps, i++, state.coordinatorId());
        ps.setLong(i++, state.epoch());
        setNullableTimestamp(ps, i++, state.lastRefreshTime());
        ps.setLong(i++, state.totalRefreshCount());
        ps.setInt(i++, state.consecutiveFailures());
        ps.setTimestamp(i++, Timestamp.from(state.updatedAt()));
        ps.setString(i++, state.dtName());
        ps.setLong(i, expectedEpoch);
    }

    // ── Row mapping ─────────────────────────────────────────────────

    private DynamicTableState mapRow(ResultSet rs) throws SQLException {
        Timestamp lastRefresh = rs.getTimestamp("last_refresh_time");
        return new DynamicTableState(
                rs.getString("dt_name"),
                rs.getString("tenant_id"),
                DynamicTableLifecycleState.valueOf(rs.getString("lifecycle_state")),
                DynamicTableRefreshMode.valueOf(rs.getString("effective_refresh_mode")),
                Duration.ofMillis(rs.getLong("target_lag_ms")),
                rs.getString("checkpoint"),
                rs.getString("checkpoint_type"),
                rs.getInt("source_schema_version"),
                rs.getString("source_schema_snapshot"),
                rs.getString("creator_identity"),
                rs.getString("coordinator_id"),
                rs.getLong("epoch"),
                lastRefresh != null ? lastRefresh.toInstant() : null,
                rs.getLong("total_refresh_count"),
                rs.getInt("consecutive_failures"),
                rs.getTimestamp("created_at").toInstant(),
                rs.getTimestamp("updated_at").toInstant());
    }

    private List<DynamicTableState> mapRows(ResultSet rs) throws SQLException {
        List<DynamicTableState> result = new ArrayList<>();
        while (rs.next()) {
            result.add(mapRow(rs));
        }
        return List.copyOf(result);
    }

    private static void bindState(PreparedStatement ps, DynamicTableState s) throws SQLException {
        int i = 1;
        ps.setString(i++, s.dtName());
        ps.setString(i++, s.tenantId());
        ps.setString(i++, s.state().name());
        ps.setString(i++, s.effectiveRefreshMode().name());
        ps.setLong(i++, s.targetLag().toMillis());
        setNullableString(ps, i++, s.checkpoint());
        setNullableString(ps, i++, s.checkpointType());
        ps.setInt(i++, s.sourceSchemaVersion());
        setNullableString(ps, i++, s.sourceSchemaSnapshot());
        setNullableString(ps, i++, s.creatorIdentity());
        setNullableString(ps, i++, s.coordinatorId());
        ps.setLong(i++, s.epoch());
        setNullableTimestamp(ps, i++, s.lastRefreshTime());
        ps.setLong(i++, s.totalRefreshCount());
        ps.setInt(i++, s.consecutiveFailures());
        ps.setTimestamp(i++, Timestamp.from(s.createdAt()));
        ps.setTimestamp(i, Timestamp.from(s.updatedAt()));
    }

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
