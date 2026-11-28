package org.pangrova.persistence.security.postgresql.policy;

import org.pangrova.persistence.security.postgresql.policy.JdbcPolicyStoreBase;

import org.pangrova.persistence.security.api.policy.GrantStore;
import org.pangrova.security.api.enums.Privilege;
import org.pangrova.security.api.policy.PrivilegeGrant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.sql.Array;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * JDBC-backed grant store persisted on the shared SQL state. Supports dynamic GRANT/REVOKE
 * operations. Schema is initialized by the canonical Schema Migrations bundle.
 *
 * <p>Per naming rule C13 this is {@code Persistent*} because {@code pangrova-security} is a policy
 * module rather than the owning persistence module.
 */
public final class JdbcGrantStore extends JdbcPolicyStoreBase implements GrantStore {

    private static final Logger LOG = LoggerFactory.getLogger(JdbcGrantStore.class);
    private static final String SELECT_GRANTS =
            "SELECT grant_id, role_name, privilege, object_type, catalog_name, schema_name, "
                    + "table_name, column_name, with_grant_option, cascade_revoke, granted_by, granted_at "
                    + "FROM security_grants";

    public JdbcGrantStore(DataSource dataSource) {
        super(dataSource);
    }

    @Override
    public List<PrivilegeGrant> listGrants() {
        String sql = SELECT_GRANTS + " ORDER BY grant_id";
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {
            return readGrants(ps);
        } catch (SQLException e) {
            throw new IllegalStateException("Failed listing all grants", e);
        }
    }

    @Override
    public List<PrivilegeGrant> listGrantsForRoles(Set<String> roleNames) {
        if (roleNames.isEmpty()) {
            return List.of();
        }
        String sql = SELECT_GRANTS + " WHERE role_name = ANY (?) ORDER BY grant_id";
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {
            Array names = conn.createArrayOf("text", roleNames.toArray(String[]::new));
            try {
                ps.setArray(1, names);
                return readGrants(ps);
            } finally {
                names.free();
            }
        } catch (SQLException e) {
            throw new IllegalStateException("Failed listing grants for effective roles", e);
        }
    }

    private static List<PrivilegeGrant> readGrants(PreparedStatement statement) throws SQLException {
        try (ResultSet rows = statement.executeQuery()) {
            return collectGrants(rows);
        }
    }

    @Override
    public void addGrant(PrivilegeGrant grant) {
        String sql = "INSERT INTO security_grants (role_name, privilege, object_type, catalog_name, "
                + "schema_name, table_name, column_name, with_grant_option, cascade_revoke, "
                + "granted_by, granted_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {
            int parameterIndex = 1;
            ps.setString(parameterIndex++, grant.roleName());
            ps.setString(parameterIndex++, grant.privilege().name());
            ps.setString(parameterIndex++, grant.objectType());
            ps.setString(parameterIndex++, grant.catalog());
            ps.setString(parameterIndex++, grant.schema());
            ps.setString(parameterIndex++, grant.table());
            ps.setString(parameterIndex++, grant.column());
            ps.setBoolean(parameterIndex++, grant.withGrantOption());
            ps.setBoolean(parameterIndex++, grant.cascade());
            ps.setString(parameterIndex++, grant.grantedBy());
            ps.setTimestamp(parameterIndex, grant.grantedAt() != null
                    ? Timestamp.from(grant.grantedAt()) : Timestamp.from(Instant.now()));
            ps.executeUpdate();
            LOG.info("event=grant_added role={} privilege={} object={}.{}.{}",
                    grant.roleName(), grant.privilege(), grant.catalog(), grant.schema(), grant.table());
        } catch (SQLException e) {
            throw new IllegalStateException("Failed adding grant for role " + grant.roleName(), e);
        }
    }

    @Override
    public boolean revokeGrant(String grantId) {
        String sql = "DELETE FROM security_grants WHERE grant_id = ?";
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setLong(1, Long.parseLong(grantId));
            int affected = ps.executeUpdate();
            if (affected > 0) {
                LOG.info("event=grant_revoked grantId={}", grantId);
            }
            return affected > 0;
        } catch (SQLException e) {
            throw new IllegalStateException("Failed revoking grant " + grantId, e);
        }
    }

    @Override
    public int revokeGrants(String roleName, Privilege privilege, String catalog, String schema, String table) {
        StringBuilder sql = new StringBuilder("DELETE FROM security_grants WHERE role_name = ? AND privilege = ?");
        List<Object> params = new ArrayList<>();
        params.add(roleName);
        params.add(privilege.name());

        appendScopeFilter(sql, params, "catalog_name", catalog);
        appendScopeFilter(sql, params, "schema_name", schema);
        appendScopeFilter(sql, params, "table_name", table);

        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql.toString())) {
            for (int i = 0; i < params.size(); i++) {
                ps.setObject(i + 1, params.get(i));
            }
            int affected = ps.executeUpdate();
            if (affected > 0) {
                LOG.info("event=grants_revoked role={} privilege={} count={}", roleName, privilege, affected);
            }
            return affected;
        } catch (SQLException e) {
            throw new IllegalStateException(
                    "Failed revoking grants for role=%s privilege=%s".formatted(roleName, privilege), e);
        }
    }

    private void appendScopeFilter(StringBuilder sql, List<Object> params, String column, String value) {
        if (value != null && !"*".equals(value)) {
            sql.append(" AND ").append(column).append(" = ?");
            params.add(value);
        }
    }

    static List<PrivilegeGrant> collectGrants(ResultSet rs) throws SQLException {
        List<PrivilegeGrant> grants = new ArrayList<>();
        while (rs.next()) {
            grants.add(readGrant(rs, rs.getLong("grant_id")));
        }
        return grants;
    }

    static PrivilegeGrant readGrant(ResultSet row, long grantId) throws SQLException {
        var grantedAt = row.getTimestamp("granted_at");
        return new PrivilegeGrant(
                String.valueOf(grantId),
                row.getString("role_name"),
                Privilege.valueOf(row.getString("privilege")),
                row.getString("object_type"),
                row.getString("catalog_name"),
                row.getString("schema_name"),
                row.getString("table_name"),
                row.getString("column_name"),
                row.getBoolean("with_grant_option"),
                row.getBoolean("cascade_revoke"),
                row.getString("granted_by"),
                grantedAt != null ? grantedAt.toInstant() : null);
    }
}
