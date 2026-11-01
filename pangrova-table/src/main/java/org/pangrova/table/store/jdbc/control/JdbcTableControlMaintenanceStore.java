package org.pangrova.table.store.jdbc.control;

import org.pangrova.common.json.CanonicalJsonMapper;
import org.pangrova.table.control.TableControlException;
import org.pangrova.table.api.maintenance.MaintenanceOperation;
import org.pangrova.table.api.maintenance.MaintenanceRunState;
import org.pangrova.table.api.maintenance.TableControlMaintenanceStore;
import org.pangrova.table.api.maintenance.TableMaintenancePolicy;
import org.pangrova.table.api.maintenance.TableMaintenanceRun;
import org.pangrova.common.concurrent.ThreadSafe;
import org.pangrova.common.jdbc.JdbcSupport;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

import static org.pangrova.common.jdbc.JdbcSupport.setJson;

/**
 * JDBC-backed maintenance policy and run history.
 *
 * <p>Authorization and the active-writer guard must agree across coordinators,
 * so any deployment that executes maintenance uses this store.</p>
 */
@ThreadSafe
public final class JdbcTableControlMaintenanceStore implements TableControlMaintenanceStore {

    private static final String FIND_POLICY =
            "SELECT table_id, enabled_operations, retain_last, orphan_file_age_seconds, "
                    + "snapshot_retention_seconds, require_stable_external_version, "
                    + "updated_by, updated_at FROM table_control_maintenance_policy "
                    + "WHERE table_id=?";

    private static final String UPSERT_POLICY =
            "INSERT INTO table_control_maintenance_policy (table_id, enabled_operations, "
                    + "retain_last, orphan_file_age_seconds, snapshot_retention_seconds, "
                    + "require_stable_external_version, updated_by, updated_at) "
                    + "VALUES (?,?,?,?,?,?,?,?) "
                    + "ON CONFLICT (table_id) DO UPDATE SET "
                    + "enabled_operations=EXCLUDED.enabled_operations, "
                    + "retain_last=EXCLUDED.retain_last, "
                    + "orphan_file_age_seconds=EXCLUDED.orphan_file_age_seconds, "
                    + "snapshot_retention_seconds=EXCLUDED.snapshot_retention_seconds, "
                    + "require_stable_external_version=EXCLUDED.require_stable_external_version, "
                    + "updated_by=EXCLUDED.updated_by, updated_at=EXCLUDED.updated_at";

    private static final String UPSERT_RUN =
            "INSERT INTO table_control_maintenance_run (run_id, table_id, operation, mode, "
                    + "state, version_before, version_after, details, actor, started_at, "
                    + "finished_at) VALUES (?,?,?,?,?,?,?,?,?,?,?) "
                    + "ON CONFLICT (run_id) DO UPDATE SET "
                    + "state=EXCLUDED.state, version_after=EXCLUDED.version_after, "
                    + "details=EXCLUDED.details, finished_at=EXCLUDED.finished_at";

    private static final String LIST_RUNS =
            "SELECT run_id, table_id, operation, mode, state, version_before, version_after, "
                    + "details, actor, started_at, finished_at "
                    + "FROM table_control_maintenance_run WHERE table_id=? "
                    + "ORDER BY started_at DESC LIMIT ?";

    private final DataSource dataSource;

    public JdbcTableControlMaintenanceStore(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    @Override
    public Optional<TableMaintenancePolicy> findPolicy(UUID tableId) {
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(FIND_POLICY)) {
            ps.setObject(1, tableId);
            try (ResultSet rs = ps.executeQuery()) {
                return rs.next() ? Optional.of(mapPolicy(rs)) : Optional.empty();
            }
        } catch (SQLException e) {
            throw new TableControlException(
                    "Failed to load maintenance policy for tableId=" + tableId, e);
        }
    }

    @Override
    public void savePolicy(TableMaintenancePolicy policy) {
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(UPSERT_POLICY)) {
            ps.setObject(1, policy.tableId());
            setJson(ps, 2, serializeOperations(policy.enabledOperations()));
            ps.setInt(3, policy.retainLast());
            ps.setLong(4, policy.orphanFileAge().toSeconds());
            ps.setLong(5, policy.snapshotRetention().toSeconds());
            ps.setBoolean(6, policy.requireStableExternalVersion());
            ps.setString(7, policy.updatedBy());
            JdbcSupport.setInstant(ps, 8, policy.updatedAt());
            ps.executeUpdate();
        } catch (SQLException e) {
            throw new TableControlException(
                    "Failed to save maintenance policy for tableId=" + policy.tableId(), e);
        }
    }

    @Override
    @SuppressWarnings("checkstyle:MagicNumber")
    public void saveRun(TableMaintenanceRun run) {
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(UPSERT_RUN)) {
            ps.setObject(1, run.runId());
            ps.setObject(2, run.tableId());
            ps.setString(3, run.operation().name());
            ps.setString(4, run.mode());
            ps.setString(5, run.state().name());
            ps.setString(6, run.versionBefore());
            ps.setString(7, run.versionAfter());
            setJson(ps, 8, serializeDetails(run.details()));
            ps.setString(9, run.actor());
            JdbcSupport.setInstant(ps, 10, run.startedAt());
            JdbcSupport.setNullableInstant(ps, 11, run.finishedAt());
            ps.executeUpdate();
        } catch (SQLException e) {
            throw new TableControlException(
                    "Failed to save maintenance run " + run.runId(), e);
        }
    }

    @Override
    public List<TableMaintenanceRun> listRuns(UUID tableId, int limit) {
        if (limit <= 0) {
            return List.of();
        }
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(LIST_RUNS)) {
            ps.setObject(1, tableId);
            ps.setInt(2, limit);
            try (ResultSet rs = ps.executeQuery()) {
                List<TableMaintenanceRun> runs = new ArrayList<>();
                while (rs.next()) {
                    runs.add(mapRun(rs));
                }
                return runs;
            }
        } catch (SQLException e) {
            throw new TableControlException(
                    "Failed to list maintenance runs for tableId=" + tableId, e);
        }
    }

    private static TableMaintenancePolicy mapPolicy(ResultSet rs) throws SQLException {
        return new TableMaintenancePolicy(
                rs.getObject("table_id", UUID.class),
                parseOperations(rs.getString("enabled_operations")),
                rs.getInt("retain_last"),
                Duration.ofSeconds(rs.getLong("orphan_file_age_seconds")),
                Duration.ofSeconds(rs.getLong("snapshot_retention_seconds")),
                rs.getBoolean("require_stable_external_version"),
                rs.getString("updated_by"),
                JdbcSupport.getInstant(rs, "updated_at"));
    }

    private static TableMaintenanceRun mapRun(ResultSet rs) throws SQLException {
        return new TableMaintenanceRun(
                rs.getObject("run_id", UUID.class),
                rs.getObject("table_id", UUID.class),
                MaintenanceOperation.valueOf(rs.getString("operation")),
                rs.getString("mode"),
                MaintenanceRunState.valueOf(rs.getString("state")),
                rs.getString("version_before"),
                rs.getString("version_after"),
                parseDetails(rs.getString("details")),
                rs.getString("actor"),
                JdbcSupport.getInstant(rs, "started_at"),
                JdbcSupport.getInstant(rs, "finished_at"));
    }

    private static String serializeOperations(Set<MaintenanceOperation> operations) {
        try {
            List<String> names = operations.stream().map(Enum::name).sorted().toList();
            return CanonicalJsonMapper.writeValueAsString(names);
        } catch (com.fasterxml.jackson.core.JsonProcessingException e) {
            throw new TableControlException("Failed to serialize maintenance operations", e);
        }
    }

    private static Set<MaintenanceOperation> parseOperations(String json) {
        if (json == null || json.isBlank()) {
            return Set.of();
        }
        try {
            List<String> names = CanonicalJsonMapper.readValue(
                    json, new com.fasterxml.jackson.core.type.TypeReference<>() { });
            EnumSet<MaintenanceOperation> operations = EnumSet.noneOf(MaintenanceOperation.class);
            for (String name : names) {
                operations.add(MaintenanceOperation.valueOf(name));
            }
            return operations;
        } catch (com.fasterxml.jackson.core.JsonProcessingException e) {
            throw new TableControlException("Failed to deserialize maintenance operations", e);
        }
    }

    private static String serializeDetails(Map<String, String> details) {
        try {
            return CanonicalJsonMapper.writeValueAsString(details);
        } catch (com.fasterxml.jackson.core.JsonProcessingException e) {
            throw new TableControlException("Failed to serialize maintenance run details", e);
        }
    }

    private static Map<String, String> parseDetails(String json) {
        if (json == null || json.isBlank()) {
            return Map.of();
        }
        try {
            return new LinkedHashMap<>(CanonicalJsonMapper.readValue(
                    json, new com.fasterxml.jackson.core.type.TypeReference<>() { }));
        } catch (com.fasterxml.jackson.core.JsonProcessingException e) {
            throw new TableControlException("Failed to deserialize maintenance run details", e);
        }
    }
}
