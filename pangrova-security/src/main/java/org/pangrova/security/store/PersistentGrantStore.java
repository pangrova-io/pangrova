package org.pangrova.security.store;

import org.pangrova.security.authz.GrantStore;
import org.pangrova.security.authz.Privilege;
import org.pangrova.security.authz.PrivilegeGrant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

/**
 * JDBC-backed grant store persisted on the shared SQL state. Supports dynamic GRANT/REVOKE
 * operations. Schema is created via {@link PersistentStoreSupport#initSchema()}.
 *
 * <p>Per naming rule C13 this is {@code Persistent*} because {@code pangrova-security} is a policy
 * module rather than the owning persistence module.
 */
public final class PersistentGrantStore extends PersistentStoreSupport implements GrantStore {

    private static final Logger LOG = LoggerFactory.getLogger(PersistentGrantStore.class);

    public PersistentGrantStore(DataSource dataSource) {
        super(dataSource);
    }

    @Override
    public List<PrivilegeGrant> listGrants() {
        String sql = "SELECT grant_id, role_name, privilege, object_type, catalog_name, schema_name, "
                + "table_name, column_name, with_grant_option, cascade_revoke, granted_by, granted_at "
                + "FROM security_grants ORDER BY grant_id";
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql);
             ResultSet rs = ps.executeQuery()) {
            return collectGrants(rs);
        } catch (SQLException e) {
            throw new IllegalStateException("Failed listing all grants", e);
        }
    }

    @Override
    public List<PrivilegeGrant> listGrantsForRole(String roleName) {
        String sql = "SELECT grant_id, role_name, privilege, object_type, catalog_name, schema_name, "
                + "table_name, column_name, with_grant_option, cascade_revoke, granted_by, granted_at "
                + "FROM security_grants WHERE role_name = ? ORDER BY grant_id";
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, roleName);
            try (ResultSet rs = ps.executeQuery()) {
                return collectGrants(rs);
            }
        } catch (SQLException e) {
            throw new IllegalStateException("Failed listing grants for role " + roleName, e);
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
            grants.add(new PrivilegeGrant(
                    String.valueOf(rs.getLong("grant_id")),
                    rs.getString("role_name"),
                    Privilege.valueOf(rs.getString("privilege")),
                    rs.getString("object_type"),
                    rs.getString("catalog_name"),
                    rs.getString("schema_name"),
                    rs.getString("table_name"),
                    rs.getString("column_name"),
                    rs.getBoolean("with_grant_option"),
                    rs.getBoolean("cascade_revoke"),
                    rs.getString("granted_by"),
                    rs.getTimestamp("granted_at") != null
                            ? rs.getTimestamp("granted_at").toInstant() : null));
        }
        return grants;
    }
}
