package org.pangrova.persistence.jdbc.security.policy;

import org.pangrova.spi.security.policy.GroupRoleMapping;
import org.pangrova.spi.security.policy.Role;
import org.pangrova.persistence.api.security.policy.RoleStore;
import org.pangrova.spi.security.policy.UserRoleAssignment;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.pangrova.testing.testkit.postgres.JdbcMutationFault;
import java.util.stream.Stream;
import static org.assertj.core.api.Assertions.catchThrowable;
import java.sql.SQLException;
import org.pangrova.testing.testkit.postgres.SharedPostgres;
import org.pangrova.testing.testkit.ContainerTestBase;

import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Validates {@link JdbcRoleStore} CRUD operations against a PostgreSQL TestContainer.
 * Mirrors the contract checks in {@link org.pangrova.security.core.authz.InMemoryRoleStoreTest}
 * so behaviour is consistent across the two {@link RoleStore} implementations.
 */
class JdbcRoleStoreTest extends ContainerTestBase {

    private JdbcRoleStore store;
    private javax.sql.DataSource dataSource;

    @BeforeEach
    void setUp() throws Exception {
        dataSource = SharedPostgres.isolatedDataSource();
        store = new JdbcRoleStore(dataSource);
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

    @Test
    void droppingAndRecreatingRoleCannotRestorePriorGrants() {
        store.createRole(Role.of("analyst"));
        store.grantRoleToUser(UserRoleAssignment.of("alice", "analyst"));
        var grants = new JdbcGrantStore(dataSource);
        grants.addGrant(org.pangrova.spi.security.policy.PrivilegeGrant.of("analyst",
                org.pangrova.spi.security.policy.Privilege.SELECT, "TABLE", "lake", "sales", "orders", null));
        assertThat(store.dropRole("analyst")).isTrue();
        store.createRole(Role.of("analyst"));
        store.grantRoleToUser(UserRoleAssignment.of("alice", "analyst"));
        assertThat(grants.listGrantsForRole("analyst")).isEmpty();
    }

    @Test
    void directSqlRoleDeletionAlsoRemovesGrantsAndBindings() throws Exception {
        store.createRole(Role.of("analyst"));
        store.grantRoleToUser(UserRoleAssignment.of("alice", "analyst"));
        var grants = new JdbcGrantStore(dataSource);
        grants.addGrant(org.pangrova.spi.security.policy.PrivilegeGrant.of("analyst",
                org.pangrova.spi.security.policy.Privilege.SELECT, "TABLE", "lake", "sales", "orders", null));
        try (var connection = dataSource.getConnection(); var statement = connection.createStatement()) {
            statement.executeUpdate("DELETE FROM pangrova_roles WHERE role_name='analyst'");
        }
        store.createRole(Role.of("analyst"));
        assertThat(store.rolesForUser("alice")).isEmpty();
        assertThat(grants.listGrantsForRole("analyst")).isEmpty();
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
        var ds = SharedPostgres.isolatedDataSource();

        JdbcRoleStore coord1 = new JdbcRoleStore(ds);
        coord1.initSchema();
        JdbcRoleStore coord2 = new JdbcRoleStore(ds);

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
    @ParameterizedTest
    @MethodSource("transactionFailures")
    void failedDropPreservesRoleGrantsAndAllBindings(Throwable failure, SQLException rollbackFailure) {
        store.createRole(Role.of("analyst"));
        store.grantRoleToUser(UserRoleAssignment.of("alice", "analyst"));
        store.mapGroupToRole(GroupRoleMapping.of("ldap-analysts", "analyst"));
        var grants = new JdbcGrantStore(dataSource);
        grants.addGrant(org.pangrova.spi.security.policy.PrivilegeGrant.of("analyst",
                org.pangrova.spi.security.policy.Privilege.SELECT, "TABLE", "lake", "sales", "orders", null));
        var originalRole = store.findRole("analyst");
        var originalGrants = grants.listGrantsForRole("analyst");
        var originalUsers = store.listUserRoles();
        var originalGroups = store.listGroupRoles();
        var failingStore = new JdbcRoleStore(JdbcMutationFault.afterMutation(dataSource,
                "DELETE FROM pangrova_group_roles ", failure, rollbackFailure));
        Throwable caught = catchThrowable(() -> failingStore.dropRole("analyst"));
        assertThat(store.findRole("analyst")).isEqualTo(originalRole);
        assertThat(grants.listGrantsForRole("analyst")).containsExactlyElementsOf(originalGrants);
        assertThat(store.listUserRoles()).containsExactlyElementsOf(originalUsers);
        assertThat(store.listGroupRoles()).containsExactlyElementsOf(originalGroups);
        assertFailure(caught, failure, rollbackFailure);
    }

    private static Stream<Arguments> transactionFailures() {
        return Stream.of(false, true).flatMap(failRollback -> Stream.of(
                new SQLException("after mutation"), new IllegalStateException("after mutation"),
                new AssertionError("after mutation")).map(failure -> Arguments.of(failure,
                        failRollback ? new SQLException("rollback unavailable") : null)));
    }

    private static void assertFailure(Throwable caught, Throwable failure, SQLException rollbackFailure) {
        if (failure instanceof SQLException) {
            assertThat(caught).isInstanceOf(IllegalStateException.class).hasCause(failure);
        } else {
            assertThat(caught).isSameAs(failure);
        }
        assertThat(failure.getSuppressed()).containsExactly(
                rollbackFailure == null ? new Throwable[0] : new Throwable[]{rollbackFailure});
    }

}
