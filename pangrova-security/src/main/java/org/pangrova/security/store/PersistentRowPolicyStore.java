package org.pangrova.security.store;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import javax.sql.DataSource;

import org.pangrova.security.rowfilter.RowPolicy;
import org.pangrova.security.rowfilter.RowPolicyStore;

/**
 * Row-level security policy store persisted on the shared SQL state (SI-4). Replaces
 * {@code EmptyRowPolicyStore} in production once RLS is enabled, so authored policies actually load
 * and enforce. Selected by {@code SecurityBootstrap} per config; until then RLS remains inert.
 */
public final class PersistentRowPolicyStore extends PersistentStoreSupport implements RowPolicyStore {

    public PersistentRowPolicyStore(DataSource dataSource) {
        super(dataSource);
    }

    @Override
    public List<RowPolicy> getPolicies(String catalog, String schema, String table) {
        String sql = "SELECT policy_id, policy_name, catalog_name, schema_name, table_name, "
                + "filter_expression, apply_to_roles, enabled, priority FROM row_policy "
                + "WHERE catalog_name = ? AND schema_name = ? AND table_name = ? AND enabled = TRUE "
                + "ORDER BY priority DESC, policy_id";
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, catalog);
            ps.setString(2, schema);
            ps.setString(3, table);
            try (ResultSet rs = ps.executeQuery()) {
                List<RowPolicy> policies = new ArrayList<>();
                while (rs.next()) {
                    policies.add(new RowPolicy(
                            rs.getLong("policy_id"),
                            rs.getString("policy_name"),
                            rs.getString("catalog_name"),
                            rs.getString("schema_name"),
                            rs.getString("table_name"),
                            rs.getString("filter_expression"),
                            decodeRoles(rs.getString("apply_to_roles")),
                            rs.getBoolean("enabled"),
                            rs.getInt("priority")));
                }
                return policies;
            }
        } catch (SQLException e) {
            throw new IllegalStateException("Failed loading row policies for "
                    + catalog + "." + schema + "." + table, e);
        }
    }

    @Override
    public void savePolicy(RowPolicy policy) {
        String sql = "INSERT INTO row_policy (policy_name, catalog_name, schema_name, table_name, "
                + "filter_expression, apply_to_roles, enabled, priority) VALUES (?, ?, ?, ?, ?, ?, ?, ?)";
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
