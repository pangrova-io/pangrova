package org.pangrova.security.authz.deferred;

import org.pangrova.persistence.jdbc.security.policy.JdbcCurrentDirectRoleReader;

import org.pangrova.persistence.jdbc.security.transaction.AuthorizationTransactionGate;

import org.pangrova.spi.security.AuthorizationDeniedException;
import org.pangrova.persistence.api.security.policy.CurrentGrantReader;
import org.pangrova.persistence.jdbc.security.policy.JdbcCurrentGrantReader;
import org.pangrova.spi.security.authz.DeferredAuthorizationScope;
import org.pangrova.spi.security.authz.DeferredAuthorizationScope.ReadMode;
import org.pangrova.spi.security.authz.DeferredAuthorizationScope.TablePermission;
import org.pangrova.spi.security.policy.Privilege;
import org.pangrova.spi.security.policy.PrivilegeGrant;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.Set;

/** Current direct-role authority for background work, read on the metadata transaction connection. */
public final class DeferredAuthorization {
    private static final JdbcCurrentDirectRoleReader CURRENT_ROLES = new JdbcCurrentDirectRoleReader();

    private static final CurrentGrantReader CURRENT_GRANTS = new JdbcCurrentGrantReader();

    private DeferredAuthorization() {}

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
        List<PrivilegeGrant> grants = CURRENT_GRANTS.readRoleGrants(connection, roles);
        for (TablePermission permission : scope.permissions()) {
            requirePermission(grants, permission);
            if (permission.readMode() != ReadMode.NONE) {
                requireCurrentPolicy(connection, principal, roles, permission, observer);
            }
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
