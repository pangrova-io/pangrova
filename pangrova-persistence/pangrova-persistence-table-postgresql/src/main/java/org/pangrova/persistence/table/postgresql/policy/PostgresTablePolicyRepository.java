package org.pangrova.persistence.table.postgresql.policy;

import static org.pangrova.persistence.jdbc.operation.JdbcOperations.rollbackAfterFailure;

import org.pangrova.common.json.CanonicalJsonMapper;
import org.pangrova.table.api.exception.TableControlException;
import org.pangrova.table.api.policy.GovernanceBinding;
import org.pangrova.table.api.enums.GovernanceBindingType;
import org.pangrova.table.api.policy.TablePolicyRepository;
import org.pangrova.common.concurrency.ThreadSafe;
import org.pangrova.persistence.jdbc.operation.JdbcOperations;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import static org.pangrova.persistence.jdbc.operation.JdbcOperations.setJson;

/**
 * Durable governance bindings keyed by {@code table_id}.
 *
 * <p>Coordinators must agree on which stable table a tag or mask belongs to, so
 * any deployment that binds governance to adopted or managed tables uses this
 * store rather than the process-local variant.</p>
 */
@ThreadSafe
public final class PostgresTablePolicyRepository
        implements TablePolicyRepository {

    private static final String UPSERT =
            "INSERT INTO table_control_governance_binding (binding_id, table_id, binding_type, "
                    + "binding_ref, config, created_by, created_at, updated_at) "
                    + "VALUES (?,?,?,?,?,?,?,?) "
                    + "ON CONFLICT (table_id, binding_type, binding_ref) DO UPDATE SET "
                    + "config=EXCLUDED.config, updated_at=EXCLUDED.updated_at "
                    + "RETURNING binding_id, table_id, binding_type, binding_ref, config, "
                    + "created_by, created_at, updated_at";

    private static final String FIND =
            "SELECT binding_id, table_id, binding_type, binding_ref, config, created_by, "
                    + "created_at, updated_at FROM table_control_governance_binding "
                    + "WHERE table_id=? AND binding_type=? AND binding_ref=?";

    private static final String LIST =
            "SELECT binding_id, table_id, binding_type, binding_ref, config, created_by, "
                    + "created_at, updated_at FROM table_control_governance_binding "
                    + "WHERE table_id=? ORDER BY binding_type, binding_ref";

    private static final String DELETE =
            "DELETE FROM table_control_governance_binding "
                    + "WHERE table_id=? AND binding_type=? AND binding_ref=?";

    private static final String DELETE_ALL =
            "DELETE FROM table_control_governance_binding WHERE table_id=?";

    private final DataSource dataSource;

    public PostgresTablePolicyRepository(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    @Override
    public GovernanceBinding save(GovernanceBinding binding) {
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(UPSERT)) {
            conn.setAutoCommit(false);
            try {
                org.pangrova.persistence.security.postgresql.transaction.AuthorizationTransactionGate.lockMutation(
                        conn);
                ps.setObject(1, binding.bindingId());
                ps.setObject(2, binding.tableId());
                ps.setString(3, binding.bindingType().name());
                ps.setString(4, binding.bindingRef());
                setJson(ps, 5, serializeConfig(binding.config()));
                ps.setString(6, binding.createdBy());
                JdbcOperations.setInstant(ps, 7, binding.createdAt());
                JdbcOperations.setInstant(ps, 8, binding.updatedAt());
                GovernanceBinding persisted;
                try (ResultSet rows = ps.executeQuery()) {
                    if (!rows.next()) throw new SQLException("Governance binding upsert returned no row");
                    persisted = readBinding(rows);
                }
                conn.commit();
                return persisted;
            } catch (SQLException | RuntimeException | Error failure) {
                rollbackAfterFailure(conn, failure);
                throw failure;
            }
        } catch (SQLException e) {
            throw new TableControlException("Failed to save governance binding "
                    + binding.bindingType() + "/" + binding.bindingRef()
                    + " for tableId=" + binding.tableId(), e);
        }
    }

    @Override
    public Optional<GovernanceBinding> find(UUID tableId, GovernanceBindingType type,
            String bindingRef) {
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(FIND)) {
            ps.setObject(1, tableId);
            ps.setString(2, type.name());
            ps.setString(3, bindingRef);
            try (ResultSet rs = ps.executeQuery()) {
                return rs.next() ? Optional.of(readBinding(rs)) : Optional.empty();
            }
        } catch (SQLException e) {
            throw new TableControlException("Failed to load governance binding "
                    + type + "/" + bindingRef + " for tableId=" + tableId, e);
        }
    }

    @Override
    public List<GovernanceBinding> list(UUID tableId) {
        try (Connection connection = dataSource.getConnection()) {
            return list(connection, tableId);
        } catch (SQLException e) {
            throw new TableControlException(
                    "Failed to list governance bindings for tableId=" + tableId, e);
        }
    }

    /** Reads policy content on the publication owner's existing transaction. */
    public static List<GovernanceBinding> list(Connection connection, UUID tableId) throws SQLException {
        try (var statement = connection.prepareStatement(LIST)) {
            statement.setObject(1, tableId);
            try (var rows = statement.executeQuery()) {
                List<GovernanceBinding> result = new ArrayList<>();
                while (rows.next()) result.add(readBinding(rows));
                return List.copyOf(result);
            }
        }
    }

    @Override
    public void delete(UUID tableId, GovernanceBindingType type, String bindingRef) {
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(DELETE)) {
            conn.setAutoCommit(false);
            try {
                org.pangrova.persistence.security.postgresql.transaction.AuthorizationTransactionGate.lockMutation(
                        conn);
                ps.setObject(1, tableId);
                ps.setString(2, type.name());
                ps.setString(3, bindingRef);
                ps.executeUpdate();
                conn.commit();
            } catch (SQLException | RuntimeException | Error failure) {
                rollbackAfterFailure(conn, failure);
                throw failure;
            }
        } catch (SQLException e) {
            throw new TableControlException("Failed to delete governance binding "
                    + type + "/" + bindingRef + " for tableId=" + tableId, e);
        }
    }

    @Override
    public void deleteAll(UUID tableId) {
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(DELETE_ALL)) {
            conn.setAutoCommit(false);
            try {
                org.pangrova.persistence.security.postgresql.transaction.AuthorizationTransactionGate.lockMutation(
                        conn);
                ps.setObject(1, tableId);
                ps.executeUpdate();
                conn.commit();
            } catch (SQLException | RuntimeException | Error failure) {
                rollbackAfterFailure(conn, failure);
                throw failure;
            }
        } catch (SQLException e) {
            throw new TableControlException(
                    "Failed to delete governance bindings for tableId=" + tableId, e);
        }
    }

    /** Decodes the governance binding columns selected by an authority query. */
    public static GovernanceBinding readBinding(ResultSet rs) throws SQLException {
        return new GovernanceBinding(
                rs.getObject("binding_id", UUID.class),
                rs.getObject("table_id", UUID.class),
                GovernanceBindingType.valueOf(rs.getString("binding_type")),
                rs.getString("binding_ref"),
                parseConfig(rs.getString("config")),
                rs.getString("created_by"),
                JdbcOperations.getInstant(rs, "created_at"),
                JdbcOperations.getInstant(rs, "updated_at"));
    }

    private static String serializeConfig(Map<String, String> config) {
        try {
            return CanonicalJsonMapper.writeValueAsString(config);
        } catch (com.fasterxml.jackson.core.JsonProcessingException e) {
            throw new TableControlException("Failed to serialize governance binding config", e);
        }
    }

    private static Map<String, String> parseConfig(String json) {
        if (json == null || json.isBlank()) {
            return Map.of();
        }
        try {
            return new LinkedHashMap<>(CanonicalJsonMapper.readValue(
                    json, new com.fasterxml.jackson.core.type.TypeReference<>() { }));
        } catch (com.fasterxml.jackson.core.JsonProcessingException e) {
            throw new TableControlException(
                    "Failed to deserialize governance binding config", e);
        }
    }
}
