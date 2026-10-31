package org.pangrova.security.store.jdbc;

import org.pangrova.security.api.AuthorizationDeniedException;
import org.pangrova.security.api.authz.DeferredAuthorizationScope;
import org.pangrova.security.api.authz.DeferredAuthorizationScope.ReadMode;
import org.pangrova.security.api.authz.DeferredAuthorizationScope.TablePermission;
import org.pangrova.security.api.policy.Privilege;
import org.pangrova.security.api.policy.PrivilegeGrant;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.Set;

/** Current direct-role authority for background work, read on the metadata transaction connection. */
public final class PersistentDeferredAuthorization {
    private static final JdbcCurrentDirectRoleReader CURRENT_ROLES = new JdbcCurrentDirectRoleReader();

    private PersistentDeferredAuthorization() {}

    /** Requires current grants and exactly the transformations observed during secure compilation. */
    public static void requireCurrent(Connection connection, String principal, DeferredAuthorizationScope scope)
            throws SQLException {
        requireCurrent(connection, principal, scope, DeferredPolicyReader::observe);
    }

    /** Metadata owners must provide their stable-identity policy projection on the same transaction. */
    public static void requireCurrent(Connection connection, String principal, DeferredAuthorizationScope scope,
            PolicyObserver observer) throws SQLException {
        if (principal == null || principal.isBlank()) {
            throw new AuthorizationDeniedException("Deferred work requires its real authenticated subject");
        }
        AuthorizationTransactionGate.lockPublication(connection);
        Set<String> roles = CURRENT_ROLES.requireCurrentRoles(connection, principal);
        List<PrivilegeGrant> grants = grants(connection, roles);
        for (TablePermission permission : scope.permissions()) {
            requirePermission(grants, permission);
            if (permission.readMode() != ReadMode.NONE) {
                requireCurrentPolicy(connection, principal, roles, permission, observer);
            }
        }
    }

    private static List<PrivilegeGrant> grants(Connection connection, Set<String> roles) throws SQLException {
        try (var statement = connection.prepareStatement("SELECT * FROM security_grants ORDER BY grant_id");
             var result = statement.executeQuery()) {
            return PersistentGrantStore.collectGrants(result).stream()
                    .filter(grant -> roles.contains(grant.roleName())).toList();
        }
    }

    private static void requirePermission(List<PrivilegeGrant> grants, TablePermission permission) {
        var table = permission.table();
        var matching = grants.stream().filter(grant -> grant.privilege().implies(permission.privilege())).toList();
        boolean allowed = permission.privilege() == Privilege.SELECT
                ? permission.columns().stream().allMatch(column -> matching.stream().anyMatch(grant ->
                        grant.coversColumn(table.catalog(), table.schema(), table.table(), column)))
                : matching.stream().anyMatch(grant ->
                        grant.coversTable(table.catalog(), table.schema(), table.table()));
        if (!allowed) {
            throw new AuthorizationDeniedException("Current deferred permission was revoked for " + table);
        }
    }

    private static void requireCurrentPolicy(Connection connection, String principal, Set<String> roles,
            TablePermission permission, PolicyObserver observer) throws SQLException {
        var current = observer.observe(connection, principal, roles, permission);
        if (permission.readMode() == ReadMode.RAW_READ) {
            if (current.rowFilter().isPresent()
                    || current.columnMasks().values().stream().anyMatch(java.util.Optional::isPresent)) {
                throw new AuthorizationDeniedException(
                        "Raw artifact reads cannot bypass current row or column policies");
            }
        } else if (!current.equals(permission.observedPolicy())) {
            throw new AuthorizationDeniedException(
                    "Deferred policy content changed; cancel and securely recompile the materialization");
        }
    }

    @FunctionalInterface
    public interface PolicyObserver {
        DeferredAuthorizationScope.PolicySnapshot observe(Connection connection, String principal, Set<String> roles,
                TablePermission permission) throws SQLException;
    }
}
