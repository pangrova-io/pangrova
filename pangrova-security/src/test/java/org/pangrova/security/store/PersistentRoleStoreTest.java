package org.pangrova.security.store;

import org.pangrova.security.authz.GroupRoleMapping;
import org.pangrova.security.authz.Role;
import org.pangrova.security.authz.RoleStore;
import org.pangrova.security.authz.UserRoleAssignment;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.PostgreSQLContainer;

import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Validates {@link PersistentRoleStore} CRUD operations against a PostgreSQL TestContainer.
 * Mirrors the contract checks in {@link org.pangrova.security.authz.InMemoryRoleStoreTest}
 * so behaviour is consistent across the two {@link RoleStore} implementations.
 */
class PersistentRoleStoreTest {

    @SuppressWarnings("resource")
    static final PostgreSQLContainer<?> PG = new PostgreSQLContainer<>("postgres:16-alpine")
            .withDatabaseName("pangrova_test")
            .withUsername("pangrova")
            .withPassword("pangrova_test");

    @BeforeAll
    static void startContainer() { PG.start(); }

    @AfterAll
    static void stopContainer() { PG.stop(); }

    private PersistentRoleStore store;

    @BeforeEach
    void setUp() throws Exception {
        org.postgresql.ds.PGSimpleDataSource ds = new org.postgresql.ds.PGSimpleDataSource();
        ds.setUrl(PG.getJdbcUrl());
        ds.setUser(PG.getUsername());
        ds.setPassword(PG.getPassword());
        // Drop and recreate tables so each test starts clean
        try (var conn = ds.getConnection()) {
            conn.createStatement().execute("DROP TABLE IF EXISTS pangrova_user_roles");
            conn.createStatement().execute("DROP TABLE IF EXISTS pangrova_group_roles");
            conn.createStatement().execute("DROP TABLE IF EXISTS pangrova_roles");
        }
        store = new PersistentRoleStore(ds);
        store.initSchema();
    }

    // ---- role catalog -------------------------------------------------------

    @Test
    void createAndFindRole() {
        Role role = new Role("analyst", "Data analyst role", "admin", Instant.now());
        boolean created = store.createRole(role);

        assertThat(created).isTrue();
        Optional<Role> found = store.findRole("analyst");
        assertThat(found).isPresent();
        assertThat(found.get().name()).isEqualTo("analyst");
        assertThat(found.get().description()).isEqualTo("Data analyst role");
        assertThat(found.get().createdBy()).isEqualTo("admin");
    }

    @Test
    void createDuplicateRoleReturnsFalse() {
        store.createRole(Role.of("analyst"));
        boolean duplicate = store.createRole(Role.of("analyst"));

        assertThat(duplicate).isFalse();
    }

    @Test
    void listRolesReturnsAllInOrder() {
        store.createRole(Role.of("beta"));
        store.createRole(Role.of("alpha"));
        store.createRole(Role.of("gamma"));

        List<Role> roles = store.listRoles();
        assertThat(roles).hasSize(3);
        assertThat(roles.get(0).name()).isEqualTo("alpha");
        assertThat(roles.get(1).name()).isEqualTo("beta");
        assertThat(roles.get(2).name()).isEqualTo("gamma");
    }

    @Test
    void findRoleReturnsEmptyForMissing() {
        assertThat(store.findRole("nonexistent")).isEmpty();
    }

    @Test
    void dropRoleRemovesRoleAndCascadesBindings() {
        store.createRole(Role.of("analyst"));
        store.grantRoleToUser(UserRoleAssignment.of("alice", "analyst"));
        store.mapGroupToRole(GroupRoleMapping.of("ldap-analysts", "analyst"));

        boolean dropped = store.dropRole("analyst");

        assertThat(dropped).isTrue();
        assertThat(store.findRole("analyst")).isEmpty();
        assertThat(store.rolesForUser("alice")).isEmpty();
        assertThat(store.rolesForGroup("ldap-analysts")).isEmpty();
        assertThat(store.listUserRoles()).isEmpty();
        assertThat(store.listGroupRoles()).isEmpty();
    }

    @Test
    void dropNonExistentRoleReturnsFalse() {
        assertThat(store.dropRole("ghost")).isFalse();
    }

    // ---- user -> role bindings ----------------------------------------------

    @Test
    void grantAndListUserRoles() {
        store.createRole(Role.of("analyst"));
        store.createRole(Role.of("engineer"));

        store.grantRoleToUser(UserRoleAssignment.of("alice", "analyst"));
        store.grantRoleToUser(UserRoleAssignment.of("alice", "engineer"));
        store.grantRoleToUser(UserRoleAssignment.of("bob", "analyst"));

        Set<String> aliceRoles = store.rolesForUser("alice");
        assertThat(aliceRoles).containsExactlyInAnyOrder("analyst", "engineer");

        Set<String> bobRoles = store.rolesForUser("bob");
        assertThat(bobRoles).containsExactly("analyst");

        List<UserRoleAssignment> all = store.listUserRoles();
        assertThat(all).hasSize(3);
    }

    @Test
    void duplicateUserRoleGrantReturnsFalse() {
        store.createRole(Role.of("analyst"));
        assertThat(store.grantRoleToUser(UserRoleAssignment.of("alice", "analyst"))).isTrue();
        assertThat(store.grantRoleToUser(UserRoleAssignment.of("alice", "analyst"))).isFalse();
        assertThat(store.listUserRoles()).hasSize(1);
    }

    @Test
    void revokeUserRole() {
        store.createRole(Role.of("analyst"));
        store.grantRoleToUser(UserRoleAssignment.of("alice", "analyst"));

        boolean revoked = store.revokeRoleFromUser("alice", "analyst");

        assertThat(revoked).isTrue();
        assertThat(store.rolesForUser("alice")).isEmpty();
    }

    @Test
    void revokeNonExistentUserRoleReturnsFalse() {
        assertThat(store.revokeRoleFromUser("alice", "ghost")).isFalse();
    }

    @Test
    void rolesForUserReturnsEmptyForUnknownUser() {
        assertThat(store.rolesForUser("unknown")).isEmpty();
    }

    // ---- group -> role bindings ---------------------------------------------

    @Test
    void mapAndListGroupRoles() {
        store.createRole(Role.of("analyst"));
        store.createRole(Role.of("viewer"));

        store.mapGroupToRole(GroupRoleMapping.of("ldap-analysts", "analyst"));
        store.mapGroupToRole(GroupRoleMapping.of("ldap-viewers", "viewer"));
        store.mapGroupToRole(GroupRoleMapping.of("ldap-analysts", "viewer"));

        Set<String> analystGroupRoles = store.rolesForGroup("ldap-analysts");
        assertThat(analystGroupRoles).containsExactlyInAnyOrder("analyst", "viewer");

        Set<String> viewerGroupRoles = store.rolesForGroup("ldap-viewers");
        assertThat(viewerGroupRoles).containsExactly("viewer");

        List<GroupRoleMapping> all = store.listGroupRoles();
        assertThat(all).hasSize(3);
    }

    @Test
    void duplicateGroupRoleMappingReturnsFalse() {
        store.createRole(Role.of("analyst"));
        assertThat(store.mapGroupToRole(GroupRoleMapping.of("g1", "analyst"))).isTrue();
        assertThat(store.mapGroupToRole(GroupRoleMapping.of("g1", "analyst"))).isFalse();
        assertThat(store.listGroupRoles()).hasSize(1);
    }

    @Test
    void unmapGroupFromRole() {
        store.createRole(Role.of("analyst"));
        store.mapGroupToRole(GroupRoleMapping.of("g1", "analyst"));

        boolean unmapped = store.unmapGroupFromRole("g1", "analyst");

        assertThat(unmapped).isTrue();
        assertThat(store.rolesForGroup("g1")).isEmpty();
    }

    @Test
    void unmapNonExistentGroupRoleReturnsFalse() {
        assertThat(store.unmapGroupFromRole("g1", "ghost")).isFalse();
    }

    @Test
    void rolesForGroupReturnsEmptyForUnknownGroup() {
        assertThat(store.rolesForGroup("unknown")).isEmpty();
    }

    // ---- cross-coordinator consistency scenario -----------------------------

    @Test
    void twoStoreInstancesSeeConsistentStateFromSameDatabase() throws Exception {
        // Simulate two coordinators sharing the same JDBC DataSource (same DB).
        org.postgresql.ds.PGSimpleDataSource ds = new org.postgresql.ds.PGSimpleDataSource();
        ds.setUrl(PG.getJdbcUrl());
        ds.setUser(PG.getUsername());
        ds.setPassword(PG.getPassword());

        PersistentRoleStore coord1 = new PersistentRoleStore(ds);
        coord1.initSchema();
        PersistentRoleStore coord2 = new PersistentRoleStore(ds);

        // Coordinator 1 creates a role and assigns a user.
        coord1.createRole(Role.of("data_engineer"));
        coord1.grantRoleToUser(UserRoleAssignment.of("bob", "data_engineer"));
        coord1.mapGroupToRole(GroupRoleMapping.of("engineering", "data_engineer"));

        // Coordinator 2 immediately sees the same state.
        assertThat(coord2.findRole("data_engineer")).isPresent();
        assertThat(coord2.rolesForUser("bob")).containsExactly("data_engineer");
        assertThat(coord2.rolesForGroup("engineering")).containsExactly("data_engineer");

        // Coordinator 2 revokes the user binding; coordinator 1 sees it.
        coord2.revokeRoleFromUser("bob", "data_engineer");
        assertThat(coord1.rolesForUser("bob")).isEmpty();
    }

    // ---- grant metadata persistence -----------------------------------------

    @Test
    void userRoleAssignmentPreservesAuditFields() {
        store.createRole(Role.of("analyst"));
        Instant now = Instant.now();
        UserRoleAssignment assignment = new UserRoleAssignment("alice", "analyst", "admin_user", now);
        store.grantRoleToUser(assignment);

        List<UserRoleAssignment> assignments = store.listUserRoles();
        assertThat(assignments).hasSize(1);
        assertThat(assignments.get(0).userId()).isEqualTo("alice");
        assertThat(assignments.get(0).roleName()).isEqualTo("analyst");
        assertThat(assignments.get(0).grantedBy()).isEqualTo("admin_user");
        assertThat(assignments.get(0).grantedAt()).isNotNull();
    }

    @Test
    void groupRoleMappingPreservesAuditFields() {
        store.createRole(Role.of("analyst"));
        Instant now = Instant.now();
        GroupRoleMapping mapping = new GroupRoleMapping("ldap-group", "analyst", "admin_user", now);
        store.mapGroupToRole(mapping);

        List<GroupRoleMapping> mappings = store.listGroupRoles();
        assertThat(mappings).hasSize(1);
        assertThat(mappings.get(0).groupName()).isEqualTo("ldap-group");
        assertThat(mappings.get(0).roleName()).isEqualTo("analyst");
        assertThat(mappings.get(0).mappedBy()).isEqualTo("admin_user");
        assertThat(mappings.get(0).mappedAt()).isNotNull();
    }
}
