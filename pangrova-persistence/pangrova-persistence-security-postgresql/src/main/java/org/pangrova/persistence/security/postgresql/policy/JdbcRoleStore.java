package org.pangrova.persistence.security.postgresql.policy;

import static org.pangrova.persistence.jdbc.operation.JdbcOperations.rollbackAfterFailure;

import org.pangrova.security.api.authorization.ReadAuthorizationSubjectSource;
import org.pangrova.security.api.policy.GroupRoleMapping;
import org.pangrova.security.api.policy.PrivilegeGrant;
import org.pangrova.security.api.policy.Role;
import org.pangrova.persistence.security.api.policy.RoleStore;
import org.pangrova.security.api.policy.UserRoleAssignment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.Locale;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;

/**
 * JDBC-backed {@link RoleStore} persisted on the shared SQL state. Provides cross-coordinator
 * consistency for the role catalog and the user/group → role bindings: every coordinator reads
 * the same authoritative tables ({@code pangrova_roles}, {@code pangrova_user_roles},
 * {@code pangrova_group_roles}) so privilege resolution does not diverge between hosts.
 *
 * <p>Schema is initialized by the canonical Schema Migrations bundle. Mutations are atomic at
 * the row level; idempotent INSERTs are guarded by primary-key collisions and duplicate keys are
 * reported as {@code false} return values rather than thrown exceptions.
 *
 * <p>Per naming rule C13 this is {@code Persistent*} because {@code pangrova-security} is a policy
 * module rather than the owning persistence module.
 */
public final class JdbcRoleStore extends JdbcPolicyStoreBase implements RoleStore, ReadAuthorizationSubjectSource {

    private static final Logger LOG = LoggerFactory.getLogger(JdbcRoleStore.class);
    private static final String EFFECTIVE_ROLES = """
            SELECT registered.role_name FROM pangrova_roles registered
            JOIN (
                SELECT role_name FROM pangrova_user_roles WHERE user_id = ?
                UNION
                SELECT role_name FROM pangrova_group_roles WHERE group_name = ANY (?)
            ) membership USING (role_name)
            """;
    private static final String READ_SUBJECT = "WITH effective AS (" + EFFECTIVE_ROLES + """
            )
            SELECT effective.role_name AS subject_role, grants.grant_id, grants.role_name, grants.privilege,
                   grants.object_type, grants.catalog_name, grants.schema_name, grants.table_name,
                   grants.column_name, grants.with_grant_option, grants.cascade_revoke,
                   grants.granted_by, grants.granted_at
            FROM effective LEFT JOIN security_grants grants USING (role_name)
            ORDER BY grants.grant_id NULLS LAST
            """;

    @Override
    public Subject read(String principal, Set<String> groups) {
        try (Connection conn = dataSource.getConnection();
             PreparedStatement statement = conn.prepareStatement(READ_SUBJECT)) {
            var groupNames = conn.createArrayOf("text", groups.toArray(String[]::new));
            try {
                statement.setString(1, principal);
                statement.setArray(2, groupNames);
                try (ResultSet rows = statement.executeQuery()) {
                    Set<String> roles = new HashSet<>();
                    List<PrivilegeGrant> grants = new ArrayList<>();
                    while (rows.next()) {
                        roles.add(rows.getString("subject_role"));
                        long grantId = rows.getLong("grant_id");
                        if (!rows.wasNull()) {
                            grants.add(JdbcGrantStore.readGrant(rows, grantId));
                        }
                    }
                    return new Subject(roles, grants);
                }
            } finally {
                groupNames.free();
            }
        } catch (SQLException failure) {
            throw new IllegalStateException("Failed resolving the read authorization subject", failure);
        }
    }

    public JdbcRoleStore(DataSource dataSource) {
        super(dataSource);
    }

    // ---- role catalog -------------------------------------------------------

    @Override
    public List<Role> listRoles() {
        String sql = "SELECT role_name, description, created_by, created_at FROM pangrova_roles ORDER BY role_name";
        List<Role> roles = new ArrayList<>();
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql);
             ResultSet rs = ps.executeQuery()) {
            while (rs.next()) {
                roles.add(toRole(rs));
            }
            return roles;
        } catch (SQLException e) {
            throw new IllegalStateException("Failed listing roles", e);
        }
    }

    @Override
    public Optional<Role> findRole(String roleName) {
        String sql = "SELECT role_name, description, created_by, created_at FROM pangrova_roles WHERE role_name = ?";
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, roleName);
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    return Optional.of(toRole(rs));
                }
                return Optional.empty();
            }
        } catch (SQLException e) {
            throw new IllegalStateException("Failed loading role " + roleName, e);
        }
    }

    @Override
    public Set<String> effectiveRoles(String principal, Set<String> groups) {
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(EFFECTIVE_ROLES)) {
            var groupNames = conn.createArrayOf("text", groups.toArray(String[]::new));
            try {
                ps.setString(1, principal);
                ps.setArray(2, groupNames);
                return readRoleNames(ps);
            } finally {
                groupNames.free();
            }
        } catch (SQLException e) {
            throw new IllegalStateException("Failed loading effective roles for " + principal, e);
        }
    }

    @Override
    public boolean createRole(Role role) {
        String sql = "INSERT INTO pangrova_roles (role_name, description, created_by, created_at) VALUES (?, ?, ?, ?)";
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, role.name());
            ps.setString(2, role.description());
            ps.setString(3, role.createdBy());
            ps.setTimestamp(4, Timestamp.from(role.createdAt() == null ? Instant.now() : role.createdAt()));
            ps.executeUpdate();
            LOG.info("event=role_created role={}", role.name());
            return true;
        } catch (SQLException e) {
            if (isDuplicateKey(e)) {
                return false;
            }
            throw new IllegalStateException("Failed creating role " + role.name(), e);
        }
    }

    @Override
    public boolean dropRole(String roleName) {
        try (Connection conn = dataSource.getConnection()) {
            conn.setAutoCommit(false);
            try {
                int affected;
                try (PreparedStatement ps = conn.prepareStatement(
                        "DELETE FROM security_grants WHERE role_name = ?")) {
                    ps.setString(1, roleName);
                    ps.executeUpdate();
                }
                try (PreparedStatement ps = conn.prepareStatement(
                        "DELETE FROM pangrova_user_roles WHERE role_name = ?")) {
                    ps.setString(1, roleName);
                    ps.executeUpdate();
                }
                try (PreparedStatement ps = conn.prepareStatement(
                        "DELETE FROM pangrova_group_roles WHERE role_name = ?")) {
                    ps.setString(1, roleName);
                    ps.executeUpdate();
                }
                try (PreparedStatement ps = conn.prepareStatement(
                        "DELETE FROM pangrova_roles WHERE role_name = ?")) {
                    ps.setString(1, roleName);
                    affected = ps.executeUpdate();
                }
                conn.commit();
                if (affected > 0) {
                    LOG.info("event=role_dropped role={}", roleName);
                }
                return affected > 0;
            } catch (SQLException | RuntimeException | Error failure) {
                rollbackAfterFailure(conn, failure);
                throw failure;
            }
        } catch (SQLException e) {
            throw new IllegalStateException("Failed dropping role " + roleName, e);
        }
    }

    // ---- user → role bindings ----------------------------------------------

    @Override
    public Set<String> rolesForUser(String userId) {
        String sql = "SELECT role_name FROM pangrova_user_roles WHERE user_id = ?";
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, userId);
            return readRoleNames(ps);
        } catch (SQLException e) {
            throw new IllegalStateException("Failed loading roles for user " + userId, e);
        }
    }

    private static Set<String> readRoleNames(PreparedStatement statement) throws SQLException {
        try (ResultSet rows = statement.executeQuery()) {
            Set<String> names = new HashSet<>();
            while (rows.next()) {
                names.add(rows.getString(1));
            }
            return Set.copyOf(names);
        }
    }

    @Override
    public List<UserRoleAssignment> listUserRoles() {
        String sql = "SELECT user_id, role_name, granted_by, granted_at FROM pangrova_user_roles "
                + "ORDER BY user_id, role_name";
        List<UserRoleAssignment> out = new ArrayList<>();
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql);
             ResultSet rs = ps.executeQuery()) {
            while (rs.next()) {
                out.add(new UserRoleAssignment(
                        rs.getString("user_id"),
                        rs.getString("role_name"),
                        rs.getString("granted_by"),
                        rs.getTimestamp("granted_at") == null ? null
                                : rs.getTimestamp("granted_at").toInstant()));
            }
            return out;
        } catch (SQLException e) {
            throw new IllegalStateException("Failed listing user roles", e);
        }
    }

    @Override
    public boolean grantRoleToUser(UserRoleAssignment assignment) {
        String sql = "INSERT INTO pangrova_user_roles (user_id, role_name, granted_by, granted_at) "
                + "VALUES (?, ?, ?, ?)";
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, assignment.userId());
            ps.setString(2, assignment.roleName());
            ps.setString(3, assignment.grantedBy());
            ps.setTimestamp(4, Timestamp.from(
                    assignment.grantedAt() == null ? Instant.now() : assignment.grantedAt()));
            ps.executeUpdate();
            LOG.info("event=user_role_granted user={} role={}", assignment.userId(), assignment.roleName());
            return true;
        } catch (SQLException e) {
            if (isDuplicateKey(e)) {
                return false;
            }
            throw new IllegalStateException(
                    "Failed granting role " + assignment.roleName() + " to user " + assignment.userId(), e);
        }
    }

    @Override
    public boolean revokeRoleFromUser(String userId, String roleName) {
        String sql = "DELETE FROM pangrova_user_roles WHERE user_id = ? AND role_name = ?";
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, userId);
            ps.setString(2, roleName);
            int affected = ps.executeUpdate();
            if (affected > 0) {
                LOG.info("event=user_role_revoked user={} role={}", userId, roleName);
            }
            return affected > 0;
        } catch (SQLException e) {
            throw new IllegalStateException(
                    "Failed revoking role " + roleName + " from user " + userId, e);
        }
    }

    // ---- group → role bindings ---------------------------------------------

    @Override
    public Set<String> rolesForGroup(String groupName) {
        String sql = "SELECT role_name FROM pangrova_group_roles WHERE group_name = ?";
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, groupName);
            return readRoleNames(ps);
        } catch (SQLException e) {
            throw new IllegalStateException("Failed loading roles for group " + groupName, e);
        }
    }

    @Override
    public List<GroupRoleMapping> listGroupRoles() {
        String sql = "SELECT group_name, role_name, mapped_by, mapped_at FROM pangrova_group_roles "
                + "ORDER BY group_name, role_name";
        List<GroupRoleMapping> out = new ArrayList<>();
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql);
             ResultSet rs = ps.executeQuery()) {
            while (rs.next()) {
                out.add(new GroupRoleMapping(
                        rs.getString("group_name"),
                        rs.getString("role_name"),
                        rs.getString("mapped_by"),
                        rs.getTimestamp("mapped_at") == null ? null
                                : rs.getTimestamp("mapped_at").toInstant()));
            }
            return out;
        } catch (SQLException e) {
            throw new IllegalStateException("Failed listing group roles", e);
        }
    }

    @Override
    public boolean mapGroupToRole(GroupRoleMapping mapping) {
        String sql = "INSERT INTO pangrova_group_roles (group_name, role_name, mapped_by, mapped_at) "
                + "VALUES (?, ?, ?, ?)";
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, mapping.groupName());
            ps.setString(2, mapping.roleName());
            ps.setString(3, mapping.mappedBy());
            ps.setTimestamp(4, Timestamp.from(
                    mapping.mappedAt() == null ? Instant.now() : mapping.mappedAt()));
            ps.executeUpdate();
            LOG.info("event=group_role_mapped group={} role={}", mapping.groupName(), mapping.roleName());
            return true;
        } catch (SQLException e) {
            if (isDuplicateKey(e)) {
                return false;
            }
            throw new IllegalStateException(
                    "Failed mapping group " + mapping.groupName() + " to role " + mapping.roleName(), e);
        }
    }

    @Override
    public boolean unmapGroupFromRole(String groupName, String roleName) {
        String sql = "DELETE FROM pangrova_group_roles WHERE group_name = ? AND role_name = ?";
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, groupName);
            ps.setString(2, roleName);
            int affected = ps.executeUpdate();
            if (affected > 0) {
                LOG.info("event=group_role_unmapped group={} role={}", groupName, roleName);
            }
            return affected > 0;
        } catch (SQLException e) {
            throw new IllegalStateException(
                    "Failed unmapping group " + groupName + " from role " + roleName, e);
        }
    }

    // ---- helpers -----------------------------------------------------------

    private static Role toRole(ResultSet rs) throws SQLException {
        return new Role(
                rs.getString("role_name"),
                rs.getString("description"),
                rs.getString("created_by"),
                rs.getTimestamp("created_at") == null ? null
                        : rs.getTimestamp("created_at").toInstant());
    }

    private static boolean isDuplicateKey(SQLException e) {
        // 23000 / 23505 are the SQL standard / PostgreSQL codes for unique violation.
        String state = e.getSQLState();
        if (state != null && state.startsWith("23")) {
            return true;
        }
        String msg = e.getMessage();
        return msg != null && (msg.contains("Duplicate") || msg.toLowerCase(Locale.ROOT).contains("unique"));
    }
}
