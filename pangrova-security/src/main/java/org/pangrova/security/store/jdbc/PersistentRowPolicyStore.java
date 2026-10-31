package org.pangrova.security.store.jdbc;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import javax.sql.DataSource;

import org.pangrova.security.api.rowfilter.RowPolicy;
import org.pangrova.security.api.rowfilter.IdentityRowPolicyStore;

/**
 * Row-level security policy store persisted on the shared SQL state (SI-4). Replaces
 * {@code EmptyRowPolicyStore} in production once RLS is enabled, so authored policies actually load
 * and enforce. Selected by {@code SecurityBootstrap} per config; until then RLS remains inert.
 */
public final class PersistentRowPolicyStore extends PersistentStoreSupport implements IdentityRowPolicyStore {

    public PersistentRowPolicyStore(DataSource dataSource) {
        super(dataSource);
    }

    private static final String SELECT_POLICIES = "SELECT policy_id, policy_name, catalog_name, schema_name, "
            + "table_name, filter_expression, apply_to_roles, enabled, priority, table_id FROM row_policy ";
    private static final String ORDER_POLICIES = " ORDER BY priority DESC, policy_id";

    @Override
    public List<RowPolicy> getPolicies(String catalog, String schema, String table) {
        return query(SELECT_POLICIES + "WHERE catalog_name=? AND schema_name=? AND table_name=? AND enabled"
                + ORDER_POLICIES, catalog, schema, table);
    }

    @Override
    public List<RowPolicy> getPolicies(java.util.UUID tableId) {
        return query(SELECT_POLICIES + "WHERE table_id=? AND enabled" + ORDER_POLICIES,
                java.util.Objects.requireNonNull(tableId, "tableId"));
    }

    private List<RowPolicy> query(String sql, Object... parameters) {
        try (Connection connection = dataSource.getConnection();
                PreparedStatement statement = connection.prepareStatement(sql)) {
            for (int index = 0; index < parameters.length; index++) statement.setObject(index + 1, parameters[index]);
            try (ResultSet row = statement.executeQuery()) {
                return read(row);
            }
        } catch (SQLException failure) {
            throw new IllegalStateException("Failed loading row policies", failure);
        }
    }

    /** Reads stable policy content without leaving a publication owner's existing transaction. */
    public static List<RowPolicy> list(Connection connection, java.util.UUID tableId) throws SQLException {
        try (var statement = connection.prepareStatement(SELECT_POLICIES
                + "WHERE table_id=? AND enabled" + ORDER_POLICIES)) {
            statement.setObject(1, java.util.Objects.requireNonNull(tableId, "tableId"));
            try (var rows = statement.executeQuery()) {
                return read(rows);
            }
        }
    }

    private static List<RowPolicy> read(ResultSet row) throws SQLException {
        List<RowPolicy> policies = new ArrayList<>();
        while (row.next()) {
            policies.add(new RowPolicy(row.getLong("policy_id"), row.getString("policy_name"),
                    row.getString("catalog_name"), row.getString("schema_name"), row.getString("table_name"),
                    row.getString("filter_expression"), decodeRoles(row.getString("apply_to_roles")),
                    row.getBoolean("enabled"), row.getInt("priority"),
                    row.getObject("table_id", java.util.UUID.class)));
        }
        return List.copyOf(policies);
    }

    private static final String INSERT_POLICY = "INSERT INTO row_policy "
            + "(policy_name, catalog_name, schema_name, table_name, "
            + "filter_expression, apply_to_roles, enabled, priority, table_id) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)";
    private static final String UPSERT_BOUND_POLICY = INSERT_POLICY
            + " ON CONFLICT (table_id, policy_name) WHERE table_id IS NOT NULL DO UPDATE SET "
            + "catalog_name=EXCLUDED.catalog_name, schema_name=EXCLUDED.schema_name, "
            + "table_name=EXCLUDED.table_name, filter_expression=EXCLUDED.filter_expression, "
            + "apply_to_roles=EXCLUDED.apply_to_roles, enabled=EXCLUDED.enabled, priority=EXCLUDED.priority";

    @Override
    public void savePolicy(RowPolicy policy) {
        String sql = policy.tableId() == null ? INSERT_POLICY : UPSERT_BOUND_POLICY;
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, policy.policyName());
            ps.setString(2, policy.catalogName());
            ps.setString(3, policy.schemaName());
            ps.setString(4, policy.tableName());
            ps.setString(5, policy.filterExpression());
            ps.setString(6, encodeRoles(policy.applyToRoles()));
            ps.setBoolean(7, policy.enabled());
            ps.setInt(8, policy.priority());
            ps.setObject(9, policy.tableId());
            ps.executeUpdate();
        } catch (SQLException e) {
            throw new IllegalStateException("Failed saving row policy " + policy.policyName(), e);
        }
    }

    @Override
    public void deletePolicy(long policyId) {
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement("DELETE FROM row_policy WHERE policy_id = ?")) {
            ps.setLong(1, policyId);
            ps.executeUpdate();
        } catch (SQLException e) {
            throw new IllegalStateException("Failed deleting row policy " + policyId, e);
        }
    }
}
