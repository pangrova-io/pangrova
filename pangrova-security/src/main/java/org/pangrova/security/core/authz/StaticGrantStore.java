package org.pangrova.security.core.authz;

import java.util.List;
import org.pangrova.security.api.policy.GrantStore;
import org.pangrova.security.api.policy.Privilege;
import org.pangrova.security.api.policy.PrivilegeGrant;

/**
 * Immutable, config-based grant store. Used as the backward-compatible fallback when no
 * persistent store is configured. Mutation operations throw {@link IllegalStateException}.
 */
public final class StaticGrantStore implements GrantStore {

    private final List<PrivilegeGrant> grants;

    public StaticGrantStore(List<PrivilegeGrant> grants) {
        this.grants = List.copyOf(grants);
    }

    @Override
    public List<PrivilegeGrant> listGrants() {
        return grants;
    }

    @Override
    public List<PrivilegeGrant> listGrantsForRole(String roleName) {
        return grants.stream()
                .filter(g -> g.roleName().equals(roleName))
                .toList();
    }

    @Override
    public void addGrant(PrivilegeGrant grant) {
        throw new IllegalStateException(
                "Static grant store does not support mutation. Configure a persistent store for GRANT/REVOKE.");
    }

    @Override
    public boolean revokeGrant(String grantId) {
        throw new IllegalStateException(
                "Static grant store does not support mutation. Configure a persistent store for GRANT/REVOKE.");
    }

    @Override
    public int revokeGrants(String roleName, Privilege privilege, String catalog, String schema, String table) {
        throw new IllegalStateException(
                "Static grant store does not support mutation. Configure a persistent store for GRANT/REVOKE.");
    }
}
