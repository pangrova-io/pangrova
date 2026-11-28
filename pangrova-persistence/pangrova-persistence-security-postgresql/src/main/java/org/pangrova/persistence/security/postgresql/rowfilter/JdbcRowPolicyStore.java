package org.pangrova.persistence.security.postgresql.rowfilter;

import org.pangrova.persistence.security.postgresql.policy.JdbcPolicyStoreBase;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import javax.sql.DataSource;

import org.pangrova.security.api.rowfilter.RowPolicy;
import org.pangrova.persistence.security.api.rowfilter.IdentityRowPolicyStore;

/**
 * Row-level security policy store persisted on the shared SQL state (SI-4). Replaces
 * {@code EmptyRowPolicyStore} in production once RLS is enabled, so authored policies actually load
 * and enforce. Selected by {@code SecurityBootstrap} per config; until then RLS remains inert.
 */
public final class JdbcRowPolicyStore extends JdbcPolicyStoreBase implements IdentityRowPolicyStore {

    public JdbcRowPolicyStore(DataSource dataSource) {
        super(dataSource);
    }

    private static final String SELECT_POLICIES = "SELECT policy_id, policy_name, catalog_name, schema_name, "
            + "table_name, filter_expression, apply_to_roles, enabled, priority, table_id FROM row_policy ";
    private static final String ORDER_POLICIES = " ORDER BY priority DESC, policy_id";
    private static final String CURRENT_TABLE_POLICIES = SELECT_POLICIES
            + "WHERE enabled AND (table_id=CAST(? AS uuid) OR "
            + "(table_id IS NULL AND catalog_name=? AND schema_name=? AND table_name=?))" + ORDER_POLICIES;

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

    @Override
    public List<RowPolicy> getPoliciesForCurrentTable(String catalog, String schema, String table,
            java.util.UUID tableId) {
        return query(CURRENT_TABLE_POLICIES, tableId, catalog, schema, table);
    }

    private List<RowPolicy> query(String sql, Object... parameters) {
        try (Connection connection = dataSource.getConnection()) {
            return query(connection, sql, parameters);
        } catch (SQLException failure) {
            throw new IllegalStateException("Failed loading row policies", failure);
        }
    }

    private static List<RowPolicy> query(Connection connection, String sql, Object... parameters) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            for (int index = 0; index < parameters.length; index++) statement.setObject(index + 1, parameters[index]);
            try (ResultSet row = statement.executeQuery()) {
                return read(row);
            }
        }
    }

    /** Reads current policy content without leaving the publication owner's transaction. */
    public static List<RowPolicy> listForCurrentTable(Connection connection, String catalog, String schema,
            String table, java.util.UUID tableId) throws SQLException {
        return query(connection, CURRENT_TABLE_POLICIES, tableId, catalog, schema, table);
    }

    private static List<RowPolicy> read(ResultSet row) throws SQLException {
        List<RowPolicy> policies = new ArrayList<>();
        while (row.next()) {
            policies.add(readRowPolicy(row));
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
