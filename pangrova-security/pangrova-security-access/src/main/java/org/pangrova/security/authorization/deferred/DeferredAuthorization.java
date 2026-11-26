package org.pangrova.security.authorization.deferred;

import org.pangrova.persistence.jdbc.api.security.policy.CurrentDirectRoleReader;
import org.pangrova.persistence.jdbc.api.security.policy.AuthorizationPublicationLock;
import org.pangrova.security.api.exception.AuthorizationDeniedException;
import org.pangrova.persistence.jdbc.api.security.policy.CurrentGrantReader;
import java.util.Objects;
import org.pangrova.security.api.authorization.DeferredAuthorizationScope;
import org.pangrova.security.api.enums.AuthorizedReadMode;
import org.pangrova.security.api.authorization.DeferredAuthorizationScope.TablePermission;
import org.pangrova.security.api.enums.Privilege;
import org.pangrova.security.api.policy.PrivilegeGrant;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.Set;

/** Current direct-role authority for background work, read on the metadata transaction connection. */
public final class DeferredAuthorization {
    private final AuthorizationPublicationLock publicationLock;
    private final CurrentDirectRoleReader currentRoles;
    private final CurrentGrantReader currentGrants;

    /** Borrows thread-safe backend collaborators; the caller retains connection ownership. */
    public DeferredAuthorization(AuthorizationPublicationLock publicationLock,
            CurrentDirectRoleReader currentRoles, CurrentGrantReader currentGrants) {
        this.publicationLock = Objects.requireNonNull(publicationLock, "publicationLock");
        this.currentRoles = Objects.requireNonNull(currentRoles, "currentRoles");
        this.currentGrants = Objects.requireNonNull(currentGrants, "currentGrants");
    }

    /** Metadata owners must provide their stable-identity policy projection on the same transaction. */
    public Set<String> requireCurrent(Connection connection, String principal, DeferredAuthorizationScope scope,
            PolicyObserver observer) throws SQLException {
        if (principal == null || principal.isBlank()) {
            throw new AuthorizationDeniedException("Deferred work requires its real authenticated subject");
        }
        publicationLock.lockPublication(connection);
        Set<String> roles = currentRoles.requireCurrentRoles(connection, principal);
        List<PrivilegeGrant> grants = currentGrants.readRoleGrants(connection, roles);
        for (TablePermission permission : scope.permissions()) {
            requirePermission(grants, permission);
            if (permission.readMode() != AuthorizedReadMode.NONE) {
                requireCurrentPolicy(connection, principal, roles, permission, observer);
            }
        }
        return roles;
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
        if (permission.readMode() == AuthorizedReadMode.RAW_READ) {
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
