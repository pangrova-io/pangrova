package org.pangrova.security.api.policy;

import java.util.List;
import java.util.Optional;
import java.util.Set;

/**
 * Backing-store SPI for the RBAC role catalog: roles plus user→role and group→role bindings.
 *
 * <p>This is the dynamic, DB-backed counterpart of the legacy static config: it lets
 * coordinators agree on a single source of truth for role membership and supports runtime
 * {@code CREATE / DROP / GRANT / REVOKE ROLE} without coordinator restarts. Implementations
 * should perform mutations atomically and return immutable snapshots from the read methods.
 */
public interface RoleStore {

    // ---- role catalog -------------------------------------------------------

    /** Returns all defined roles. */
    List<Role> listRoles();

    /** Looks up a single role by name. */
    Optional<Role> findRole(String roleName);

    /**
     * Persists a new role. Idempotent: returns {@code false} when a role with the same name already exists.
     *
     * @return true if the role was created, false if it was already present
     */
    boolean createRole(Role role);

    /**
     * Removes a role and cascades all user-role / group-role mappings that reference it.
     *
     * @return true if the role was removed, false if no such role existed
     */
    boolean dropRole(String roleName);

    // ---- user → role bindings ----------------------------------------------

    /** Returns the role names directly granted to {@code userId}. */
    Set<String> rolesForUser(String userId);

    /** Returns all user → role bindings (admin / introspection use). */
    List<UserRoleAssignment> listUserRoles();

    /** Adds a user-role assignment. Idempotent: returns {@code false} on duplicate. */
    boolean grantRoleToUser(UserRoleAssignment assignment);

    /** Removes a user-role assignment. Returns {@code false} if it did not exist. */
    boolean revokeRoleFromUser(String userId, String roleName);

    // ---- group → role bindings ---------------------------------------------

    /** Returns the role names mapped to {@code groupName}. */
    Set<String> rolesForGroup(String groupName);

    /** Returns all group → role mappings. */
    List<GroupRoleMapping> listGroupRoles();

    /** Adds a group-role mapping. Idempotent: returns {@code false} on duplicate. */
    boolean mapGroupToRole(GroupRoleMapping mapping);

    /** Removes a group-role mapping. Returns {@code false} if it did not exist. */
    boolean unmapGroupFromRole(String groupName, String roleName);
}
