package org.pangrova.persistence.security.api.policy;

import org.pangrova.common.api.annotation.Experimental;

import org.pangrova.security.api.enums.Privilege;
import org.pangrova.security.api.policy.PrivilegeGrant;

import java.util.List;
import java.util.Set;

/**
 * Abstraction over the backing store for privilege grants. Implementations may be static
 * (in-memory, config-based) or persistent (JDBC-backed with dynamic GRANT/REVOKE support).
 */
@Experimental
public interface GrantStore {

    /**
     * Returns all grants currently stored. Implementations should return an unmodifiable snapshot.
     */
    List<PrivilegeGrant> listGrants();

    /**
     * Returns a single snapshot of grants held by any of the supplied roles.
     */
    List<PrivilegeGrant> listGrantsForRoles(Set<String> roleNames);

    /**
     * Persists a new grant. Throws if the grant already exists.
     *
     * @param grant the privilege grant to add
     * @throws IllegalStateException if the store does not support mutation (e.g. static config)
     */
    void addGrant(PrivilegeGrant grant);

    /**
     * Revokes (removes) a grant by its unique identifier.
     *
     * @param grantId the grant identifier
     * @return true if the grant was found and removed, false otherwise
     * @throws IllegalStateException if the store does not support mutation (e.g. static config)
     */
    boolean revokeGrant(String grantId);

    /**
     * Revokes all grants matching the given role, privilege, and object coordinates.
     *
     * @return the number of grants revoked
     * @throws IllegalStateException if the store does not support mutation (e.g. static config)
     */
    int revokeGrants(String roleName, Privilege privilege, String catalog, String schema, String table);
}
