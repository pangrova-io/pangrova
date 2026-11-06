package org.pangrova.persistence.security.postgresql.grant;

import org.pangrova.security.api.grant.PrivilegeMutation;
import org.pangrova.security.api.grant.PrivilegeScope;
import org.pangrova.security.api.policy.Privilege;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

/** Exact-scope writes and delegation closure, using only the workflow-owned connection. */
final class PrivilegeGrantRows {
    private static final String EXACT_SCOPE = "object_type=? AND catalog_name IS NOT DISTINCT FROM ? "
            + "AND schema_name IS NOT DISTINCT FROM ? AND table_name IS NOT DISTINCT FROM ? AND column_name IS NULL";

    private PrivilegeGrantRows() {}

    static int grant(Connection connection, String principal, String role, Privilege privilege,
            PrivilegeScope scope, boolean option, long parent) throws SQLException {
        String select = "SELECT grant_id, with_grant_option FROM security_grants WHERE role_name=? AND privilege=? "
                + "AND " + EXACT_SCOPE + " AND granted_by=? AND parent_grant_id=? ORDER BY grant_id";
        try (var query = connection.prepareStatement(select)) {
            query.setString(1, role);
            query.setString(2, privilege.name());
            bindScope(query, 3, scope);
            query.setString(7, principal);
            query.setLong(8, parent);
            try (var rows = query.executeQuery()) {
                if (rows.next()) {
                    if (!option || rows.getBoolean(2)) return 0;
                    long id = rows.getLong(1);
                    try (var update = connection.prepareStatement(
                            "UPDATE security_grants SET with_grant_option=TRUE WHERE grant_id=?")) {
                        update.setLong(1, id);
                        return update.executeUpdate();
                    }
                }
            }
        }
        String sql = "INSERT INTO security_grants (role_name, privilege, object_type, catalog_name, schema_name, "
                + "table_name, with_grant_option, granted_by, parent_grant_id) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)";
        try (var insert = connection.prepareStatement(sql)) {
            insert.setString(1, role);
            insert.setString(2, privilege.name());
            bindScope(insert, 3, scope);
            insert.setBoolean(7, option);
            insert.setString(8, principal);
            insert.setLong(9, parent);
            return insert.executeUpdate();
        }
    }

    static int revoke(Connection connection, String principal, PrivilegeMutation mutation, boolean owner)
            throws SQLException {
        Set<Long> selected = new LinkedHashSet<>();
        String sql = "SELECT grant_id, privilege, granted_by, with_grant_option FROM security_grants WHERE "
                + EXACT_SCOPE + " AND role_name=? ORDER BY grant_id";
        try (var query = connection.prepareStatement(sql)) {
            bindScope(query, 1, mutation.scope());
            for (var role : mutation.roles().stream().sorted().toList()) {
                query.setString(5, role);
                try (var rows = query.executeQuery()) {
                    while (rows.next()) {
                        var privilege = Privilege.valueOf(rows.getString(2));
                        if (privilege == Privilege.OWNER || (!owner && !principal.equals(rows.getString(3)))) continue;
                        if (!mutation.privileges().contains(Privilege.ALL)
                                && !mutation.privileges().contains(privilege)) continue;
                        if (mutation.operation() == PrivilegeMutation.Operation.REVOKE_GRANT_OPTION
                                && !rows.getBoolean(4)) continue;
                        selected.add(rows.getLong(1));
                    }
                }
            }
        }
        if (selected.isEmpty()) return 0;
        Set<Long> descendants = descendants(connection, selected);
        Set<Long> restricted = new LinkedHashSet<>(descendants);
        if (mutation.operation() == PrivilegeMutation.Operation.REVOKE) restricted.removeAll(selected);
        if (!restricted.isEmpty() && mutation.revokeBehavior() == PrivilegeMutation.RevokeBehavior.RESTRICT) {
            throw new IllegalStateException("Dependent privilege grants exist; use REVOKE CASCADE");
        }
        selected.removeAll(descendants);
        int changed = descendants.size();
        // Delete descendants before changing grant-option so the database's defensive trigger is a no-op.
        delete(connection, descendants);
        if (mutation.operation() == PrivilegeMutation.Operation.REVOKE) {
            changed += selected.size();
            delete(connection, selected);
        } else {
            try (var update = connection.prepareStatement(
                    "UPDATE security_grants SET with_grant_option=FALSE WHERE grant_id=?")) {
                for (long id : selected) {
                    update.setLong(1, id);
                    changed += update.executeUpdate();
                }
            }
        }
        return changed;
    }

    private static Set<Long> descendants(Connection connection, Set<Long> roots) throws SQLException {
        var array = connection.createArrayOf("bigint", roots.toArray());
        try (var query = connection.prepareStatement("WITH RECURSIVE descendants(id) AS ("
                + "SELECT grant_id FROM security_grants WHERE parent_grant_id=ANY(?) UNION "
                + "SELECT g.grant_id FROM security_grants g JOIN descendants d ON g.parent_grant_id=d.id) "
                + "SELECT id FROM descendants")) {
            query.setArray(1, array);
            try (var rows = query.executeQuery()) {
                Set<Long> result = new LinkedHashSet<>();
                while (rows.next()) result.add(rows.getLong(1));
                return result;
            }
        } finally {
            array.free();
        }
    }

    private static void delete(Connection connection, Set<Long> ids) throws SQLException {
        // Children have larger IDs by the schema invariant. Delete leaf-first to avoid redundant cascades.
        List<Long> descending = new ArrayList<>(ids);
        descending.sort(java.util.Comparator.reverseOrder());
        try (var delete = connection.prepareStatement("DELETE FROM security_grants WHERE grant_id=?")) {
            for (long id : descending) {
                delete.setLong(1, id);
                delete.addBatch();
            }
            delete.executeBatch();
        }
    }

    private static void bindScope(PreparedStatement statement, int offset, PrivilegeScope scope)
            throws SQLException {
        statement.setString(offset, scope.objectType().name());
        statement.setString(offset + 1, scope.catalog());
        statement.setString(offset + 2, scope.schema());
        statement.setString(offset + 3, scope.object());
    }
}
