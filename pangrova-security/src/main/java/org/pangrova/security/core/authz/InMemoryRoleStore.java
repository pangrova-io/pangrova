package org.pangrova.security.core.authz;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import org.pangrova.security.api.policy.GroupRoleMapping;
import org.pangrova.security.api.policy.Role;
import org.pangrova.security.api.policy.RoleStore;
import org.pangrova.security.api.policy.UserRoleAssignment;

/**
 * Thread-safe in-memory {@link RoleStore} for tests and dev/standalone deployments
 * where no shared SQL state is configured. Mutations are atomic at the per-key level;
 * read methods return defensive copies.
 *
 * <p>Production deployments should use {@link org.pangrova.security.store.jdbc.PersistentRoleStore}
 * for cross-coordinator consistency.
 */
public final class InMemoryRoleStore implements RoleStore {

    private final ConcurrentHashMap<String, Role> roles = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, Set<String>> userRoles = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, Set<String>> groupRoles = new ConcurrentHashMap<>();
    private final List<UserRoleAssignment> userAssignments =
            Collections.synchronizedList(new ArrayList<>());
    private final List<GroupRoleMapping> groupMappings =
            Collections.synchronizedList(new ArrayList<>());

    @Override
    public List<Role> listRoles() {
        return List.copyOf(roles.values());
    }

    @Override
    public Optional<Role> findRole(String roleName) {
        return Optional.ofNullable(roles.get(roleName));
    }

    @Override
    public boolean createRole(Role role) {
        return roles.putIfAbsent(role.name(), role) == null;
    }

    @Override
    public boolean dropRole(String roleName) {
        if (roles.remove(roleName) == null) {
            return false;
        }
        userRoles.values().forEach(s -> s.remove(roleName));
        groupRoles.values().forEach(s -> s.remove(roleName));
        synchronized (userAssignments) {
            userAssignments.removeIf(a -> a.roleName().equals(roleName));
        }
        synchronized (groupMappings) {
            groupMappings.removeIf(m -> m.roleName().equals(roleName));
        }
        return true;
    }

    @Override
    public Set<String> rolesForUser(String userId) {
        Set<String> set = userRoles.get(userId);
        return set == null ? Set.of() : Set.copyOf(set);
    }

    @Override
    public List<UserRoleAssignment> listUserRoles() {
        synchronized (userAssignments) {
            return List.copyOf(userAssignments);
        }
    }

    @Override
    public boolean grantRoleToUser(UserRoleAssignment assignment) {
        Set<String> set = userRoles.computeIfAbsent(assignment.userId(),
                k -> Collections.newSetFromMap(new ConcurrentHashMap<>()));
        boolean added = set.add(assignment.roleName());
        if (added) {
            userAssignments.add(assignment);
        }
        return added;
    }

    @Override
    public boolean revokeRoleFromUser(String userId, String roleName) {
        Set<String> set = userRoles.get(userId);
        if (set == null || !set.remove(roleName)) {
            return false;
        }
        synchronized (userAssignments) {
            userAssignments.removeIf(a -> a.userId().equals(userId) && a.roleName().equals(roleName));
        }
        return true;
    }

    @Override
    public Set<String> rolesForGroup(String groupName) {
        Set<String> set = groupRoles.get(groupName);
        return set == null ? Set.of() : Set.copyOf(set);
    }

    @Override
    public List<GroupRoleMapping> listGroupRoles() {
        synchronized (groupMappings) {
            return List.copyOf(groupMappings);
        }
    }

    @Override
    public boolean mapGroupToRole(GroupRoleMapping mapping) {
        Set<String> set = groupRoles.computeIfAbsent(mapping.groupName(),
                k -> Collections.newSetFromMap(new ConcurrentHashMap<>()));
        boolean added = set.add(mapping.roleName());
        if (added) {
            groupMappings.add(mapping);
        }
        return added;
    }

    @Override
    public boolean unmapGroupFromRole(String groupName, String roleName) {
        Set<String> set = groupRoles.get(groupName);
        if (set == null || !set.remove(roleName)) {
            return false;
        }
        synchronized (groupMappings) {
            groupMappings.removeIf(m -> m.groupName().equals(groupName) && m.roleName().equals(roleName));
        }
        return true;
    }

    /**
     * Computes the union of user-role and group-role bindings for the given identity.
     * This helper centralises the "group != role" resolution rule.
     */
    public Set<String> resolveRoles(String userId, Set<String> groups) {
        Set<String> resolved = new HashSet<>(rolesForUser(userId));
        if (groups != null) {
            for (String group : groups) {
                resolved.addAll(rolesForGroup(group));
            }
        }
        return Set.copyOf(resolved);
    }
}
