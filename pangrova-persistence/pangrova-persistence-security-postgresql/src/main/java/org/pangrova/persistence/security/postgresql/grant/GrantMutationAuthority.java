package org.pangrova.persistence.security.postgresql.grant;

import org.pangrova.security.api.exception.AuthorizationDeniedException;
import org.pangrova.security.api.grant.PrivilegeScope;
import org.pangrova.security.api.identity.UserIdentity;
import org.pangrova.security.api.enums.Privilege;
import org.pangrova.security.api.policy.PrivilegeGrant;
import org.pangrova.persistence.security.postgresql.policy.JdbcCurrentGrantReader;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;

/** Current authority reads on the workflow's locked connection; never owns its transaction. */
final class GrantMutationAuthority {
    private GrantMutationAuthority() {}

    static List<PrivilegeGrant> read(Connection connection, UserIdentity actor) throws SQLException {
        Set<String> roles = new HashSet<>();
        try (var query = connection.prepareStatement("SELECT a.role_name FROM pangrova_user_roles a "
                + "JOIN pangrova_roles r ON r.role_name=a.role_name WHERE a.user_id=?")) {
            query.setString(1, actor.principal());
            try (var rows = query.executeQuery()) {
                while (rows.next()) roles.add(rows.getString(1));
            }
        }
        try (var query = connection.prepareStatement("SELECT a.role_name, a.group_name FROM pangrova_group_roles a "
                + "JOIN pangrova_roles r ON r.role_name=a.role_name")) {
            try (var rows = query.executeQuery()) {
                while (rows.next()) {
                    if (actor.groups().contains(rows.getString(2))) roles.add(rows.getString(1));
                }
            }
        }
        return new JdbcCurrentGrantReader().readRoleGrants(connection, roles);
    }

    static PrivilegeGrant requireSupportingGrant(List<PrivilegeGrant> grants, Privilege privilege,
            PrivilegeScope scope) {
        return grants.stream().filter(grant -> covers(grant, scope))
                .filter(grant -> grant.privilege().implies(privilege))
                .filter(grant -> grant.withGrantOption() || grant.privilege() == Privilege.OWNER)
                .sorted(java.util.Comparator.comparing((PrivilegeGrant g) -> g.privilege() != Privilege.OWNER)
                        .thenComparingLong(g -> Long.parseLong(g.id())))
                .findFirst().orElseThrow(() -> new AuthorizationDeniedException(
                        "Current OWNER or " + privilege + " WITH GRANT OPTION is required on " + scope));
    }

    static boolean owns(List<PrivilegeGrant> grants, PrivilegeScope scope) {
        return grants.stream().anyMatch(grant -> grant.privilege() == Privilege.OWNER && covers(grant, scope));
    }

    /** Containment, not a point match: a narrow grant can never authorize a broader scope. */
    static boolean covers(PrivilegeGrant grant, PrivilegeScope scope) {
        if (grant.isColumnScoped()) return false;
        String kind = grant.objectType().toUpperCase(Locale.ROOT);
        boolean compatible = switch (scope.objectType()) {
            case STORAGE_PROFILE -> kind.equals("STORAGE_PROFILE");
            case CATALOG -> kind.equals("CATALOG");
            case SCHEMA -> kind.equals("CATALOG") || kind.equals("SCHEMA");
            case TABLE -> kind.equals("CATALOG") || kind.equals("SCHEMA") || kind.equals("TABLE");
            case VOLUME -> kind.equals("CATALOG") || kind.equals("SCHEMA") || kind.equals("VOLUME");
            case STAGE -> kind.equals("CATALOG") || kind.equals("SCHEMA") || kind.equals("STAGE");
        };
        return compatible && contains(grant.catalog(), scope.catalog())
                && contains(grant.schema(), scope.schema()) && contains(grant.table(), scope.object());
    }

    private static boolean contains(String parent, String child) {
        return parent == null || parent.equals("*") || parent.equals(child);
    }

    static void requireRoles(Connection connection, Set<String> roles) throws SQLException {
        try (var query = connection.prepareStatement("SELECT 1 FROM pangrova_roles WHERE role_name=?")) {
            for (var role : roles) {
                query.setString(1, role);
                try (var rows = query.executeQuery()) {
                    if (!rows.next()) throw new IllegalArgumentException("Unknown recipient role: " + role);
                }
            }
        }
    }
}
