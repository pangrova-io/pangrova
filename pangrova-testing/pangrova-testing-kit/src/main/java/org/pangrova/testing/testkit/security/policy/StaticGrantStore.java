package org.pangrova.testing.testkit.security.policy;

import java.util.List;
import java.util.Set;
import org.pangrova.persistence.security.api.policy.GrantStore;
import org.pangrova.security.api.enums.Privilege;
import org.pangrova.security.api.policy.PrivilegeGrant;

/**
 * Immutable grant fixture for authorization tests. Mutation operations throw {@link IllegalStateException}.
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
    public List<PrivilegeGrant> listGrantsForRoles(Set<String> roleNames) {
        return grants.stream()
                .filter(g -> roleNames.contains(g.roleName()))
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
