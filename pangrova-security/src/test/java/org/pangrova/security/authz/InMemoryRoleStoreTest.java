package org.pangrova.security.authz;

import org.junit.jupiter.api.Test;

import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Validates the in-memory {@link RoleStore} used by tests and standalone deployments.
 * Mirrors the contract checks in {@code PersistentRoleStoreTest} so behaviour is consistent
 * across the two implementations.
 */
class InMemoryRoleStoreTest {

    @Test
    void resolveRolesUnionsUserAndGroupBindings() {
        InMemoryRoleStore store = new InMemoryRoleStore();
        store.createRole(Role.of("r-direct"));
        store.createRole(Role.of("r-via-group"));
        store.grantRoleToUser(UserRoleAssignment.of("alice", "r-direct"));
        store.mapGroupToRole(GroupRoleMapping.of("g1", "r-via-group"));

        Set<String> roles = store.resolveRoles("alice", Set.of("g1"));
        assertThat(roles).containsExactlyInAnyOrder("r-direct", "r-via-group");
    }

    @Test
    void groupNotMappedReturnsEmpty() {
        InMemoryRoleStore store = new InMemoryRoleStore();
        store.createRole(Role.of("analyst"));
        // No group→role mapping. A group called "analyst" is NOT the role "analyst".
        Set<String> roles = store.resolveRoles("alice", Set.of("analyst"));
        assertThat(roles).isEmpty();
    }

    @Test
    void duplicateGrantIsIdempotent() {
        InMemoryRoleStore store = new InMemoryRoleStore();
        store.createRole(Role.of("r"));
        assertThat(store.grantRoleToUser(UserRoleAssignment.of("u", "r"))).isTrue();
        assertThat(store.grantRoleToUser(UserRoleAssignment.of("u", "r"))).isFalse();
        assertThat(store.listUserRoles()).hasSize(1);
    }

    @Test
    void dropRoleRemovesAllBindings() {
        InMemoryRoleStore store = new InMemoryRoleStore();
        store.createRole(Role.of("r"));
        store.grantRoleToUser(UserRoleAssignment.of("u", "r"));
        store.mapGroupToRole(GroupRoleMapping.of("g", "r"));

        assertThat(store.dropRole("r")).isTrue();
        assertThat(store.findRole("r")).isEmpty();
        assertThat(store.rolesForUser("u")).isEmpty();
        assertThat(store.rolesForGroup("g")).isEmpty();
        assertThat(store.listUserRoles()).isEmpty();
        assertThat(store.listGroupRoles()).isEmpty();
    }

    @Test
    void revokeIsIdempotent() {
        InMemoryRoleStore store = new InMemoryRoleStore();
        store.createRole(Role.of("r"));
        store.grantRoleToUser(UserRoleAssignment.of("u", "r"));
        assertThat(store.revokeRoleFromUser("u", "r")).isTrue();
        assertThat(store.revokeRoleFromUser("u", "r")).isFalse();
    }
}
