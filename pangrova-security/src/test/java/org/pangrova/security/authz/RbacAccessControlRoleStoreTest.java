package org.pangrova.security.authz;

import org.pangrova.security.mask.ColumnMaskingProcessor;
import org.pangrova.security.mask.EmptyColumnMaskStore;
import org.pangrova.security.rowfilter.EmptyRowPolicyStore;
import org.pangrova.security.rowfilter.RowLevelSecurityFilter;
import org.pangrova.spi.security.AccessDeniedException;
import org.pangrova.spi.security.UserIdentity;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * End-to-end test for the role-resolved, dynamic RBAC introduced by F-SEC-04
 * via {@link DynamicRbacAccessControl} backed by {@link InMemoryRoleStore} and a
 * mutable in-process {@link GrantStore}.
 *
 * <p>JDBC behaviour is exercised separately by {@code PersistentRoleStoreTest} and
 * {@code PersistentGrantStoreTest} — this test focuses on the semantic model:
 * <ul>
 *   <li>Group is NOT treated as role: a user whose IdP group name happens to match a role
 *       name is denied unless an explicit group→role mapping exists.</li>
 *   <li>Direct user→role bindings work even when the user has no groups.</li>
 *   <li>Group→role mapping resolves through the role store.</li>
 *   <li>Dynamic GRANT/REVOKE of role membership takes effect immediately.</li>
 *   <li>{@link Privilege#ALL} grants satisfy any privilege check on the bound scope.</li>
 *   <li>Two access-control instances sharing the same store read current authorization on every check.</li>
 * </ul>
 */
class RbacAccessControlRoleStoreTest {

    private MutableGrantStore grantStore;
    private InMemoryRoleStore roleStore;
    private DynamicRbacAccessControl accessControl;

    @BeforeEach
    void setUp() {
        grantStore = new MutableGrantStore();
        roleStore = new InMemoryRoleStore();
        accessControl = new DynamicRbacAccessControl(
                grantStore,
                roleStore,
                new RowLevelSecurityFilter(new EmptyRowPolicyStore()),
                new ColumnMaskingProcessor(new EmptyColumnMaskStore()));
    }

    private static UserIdentity userWithGroups(String principal, String... groups) {
        return new UserIdentity(principal, Set.of(groups), Map.of(), Instant.now());
    }

    @Test
    void catalogReadAndTableOwnershipCannotAuthorizeCatalogMutation() {
        roleStore.createRole(Role.of("reader"));
        roleStore.grantRoleToUser(UserRoleAssignment.of("alice", "reader"));
        grantStore.addGrant(PrivilegeGrant.of("reader", Privilege.SELECT, "CATALOG", "lake", null, null, null));
        grantStore.addGrant(PrivilegeGrant.of("reader", Privilege.ALL, "TABLE", "lake", "*", "*", null));

        var alice = userWithGroups("alice");
        assertThatCode(() -> accessControl.checkCanAccessCatalog(alice, "lake")).doesNotThrowAnyException();
        assertThatThrownBy(() -> accessControl.checkCanCreateCatalog(alice, "lake"))
                .isInstanceOf(AccessDeniedException.class);
        assertThatThrownBy(() -> accessControl.checkCanDropCatalog(alice, "lake"))
                .isInstanceOf(AccessDeniedException.class);
        assertThatThrownBy(() -> accessControl.checkCanAlterCatalog(alice, "lake"))
                .isInstanceOf(AccessDeniedException.class);
    }

    @Test
    void catalogMutationRequiresTheExactActionAndCatalog() {
        roleStore.createRole(Role.of("creator"));
        roleStore.grantRoleToUser(UserRoleAssignment.of("alice", "creator"));
        grantStore.addGrant(PrivilegeGrant.of(
                "creator", Privilege.CREATE_CATALOG, "CATALOG", "lake", null, null, null));

        var alice = userWithGroups("alice");
        assertThatCode(() -> accessControl.checkCanCreateCatalog(alice, "lake")).doesNotThrowAnyException();
        assertThatThrownBy(() -> accessControl.checkCanCreateCatalog(alice, "other"))
                .isInstanceOf(AccessDeniedException.class);
        assertThatThrownBy(() -> accessControl.checkCanDropCatalog(alice, "lake"))
                .isInstanceOf(AccessDeniedException.class);
        assertThatThrownBy(() -> accessControl.checkCanAlterCatalog(alice, "lake"))
                .isInstanceOf(AccessDeniedException.class);
        grantStore.addGrant(PrivilegeGrant.of("creator", Privilege.ALL, "CATALOG", "lake", null, null, null));

        assertThatCode(() -> accessControl.checkCanDropCatalog(alice, "lake")).doesNotThrowAnyException();
        assertThatCode(() -> accessControl.checkCanAlterCatalog(alice, "lake")).doesNotThrowAnyException();
    }

    @Test
    void groupNameMatchingRoleNameIsNotEnoughWithoutMapping() {
        // The legacy List-of-grants RbacAccessControl treated group=="analyst" as role "analyst".
        // The DB-backed model with RoleStore must NOT do that — group is identity, not RBAC.
        roleStore.createRole(Role.of("analyst"));
        grantStore.addGrant(PrivilegeGrant.of(
                "analyst", Privilege.SELECT, "TABLE", "iceberg", "default", "orders", null));


        UserIdentity user = userWithGroups("alice", "analyst"); // group "analyst", no mapping
        assertThatThrownBy(() ->
                accessControl.checkCanSelectFromTable(user, "iceberg", "default", "orders"))
                .isInstanceOf(AccessDeniedException.class);
    }

    @Test
    void groupRoleMappingResolvesPrivilege() {
        roleStore.createRole(Role.of("analyst"));
        roleStore.mapGroupToRole(GroupRoleMapping.of("ldap-analysts", "analyst"));
        grantStore.addGrant(PrivilegeGrant.of(
                "analyst", Privilege.SELECT, "TABLE", "iceberg", "default", "orders", null));


        UserIdentity user = userWithGroups("alice", "ldap-analysts");
        assertThatCode(() ->
                accessControl.checkCanSelectFromTable(user, "iceberg", "default", "orders"))
                .doesNotThrowAnyException();
    }

    @Test
    void directUserRoleAssignmentResolvesPrivilege() {
        roleStore.createRole(Role.of("data_engineer"));
        roleStore.grantRoleToUser(UserRoleAssignment.of("bob", "data_engineer"));
        grantStore.addGrant(PrivilegeGrant.of(
                "data_engineer", Privilege.INSERT, "TABLE", "iceberg", "default", "events", null));


        UserIdentity user = new UserIdentity("bob", Set.of(), Map.of(), Instant.now());
        assertThatCode(() ->
                accessControl.checkCanInsertIntoTable(user, "iceberg", "default", "events"))
                .doesNotThrowAnyException();
    }

    @Test
    void privilegeAllSatisfiesAnyCheck() {
        roleStore.createRole(Role.of("admin"));
        roleStore.grantRoleToUser(UserRoleAssignment.of("root", "admin"));
        grantStore.addGrant(PrivilegeGrant.of(
                "admin", Privilege.ALL, "TABLE", "*", "*", "*", null));


        UserIdentity admin = new UserIdentity("root", Set.of(), Map.of(), Instant.now());
        assertThatCode(() -> {
            accessControl.checkCanSelectFromTable(admin, "iceberg", "default", "orders");
            accessControl.checkCanInsertIntoTable(admin, "iceberg", "default", "orders");
            accessControl.checkCanDeleteFromTable(admin, "iceberg", "default", "orders");
            accessControl.checkCanDropTable(admin, "iceberg", "default", "orders");
        }).doesNotThrowAnyException();
    }

    @Test
    void dynamicRoleRevocationTakesEffectImmediately() {
        roleStore.createRole(Role.of("analyst"));
        roleStore.grantRoleToUser(UserRoleAssignment.of("alice", "analyst"));
        grantStore.addGrant(PrivilegeGrant.of(
                "analyst", Privilege.SELECT, "TABLE", "iceberg", "default", "orders", null));


        UserIdentity alice = new UserIdentity("alice", Set.of(), Map.of(), Instant.now());
        assertThatCode(() ->
                accessControl.checkCanSelectFromTable(alice, "iceberg", "default", "orders"))
                .doesNotThrowAnyException();

        // Revoke the current assignment without an event or cache refresh.
        roleStore.revokeRoleFromUser("alice", "analyst");

        assertThatThrownBy(() ->
                accessControl.checkCanSelectFromTable(alice, "iceberg", "default", "orders"))
                .isInstanceOf(AccessDeniedException.class);
    }

    @Test
    void effectiveRolesUnionsUserAndGroupBindings() {
        roleStore.createRole(Role.of("r-direct"));
        roleStore.createRole(Role.of("r-via-group"));
        roleStore.grantRoleToUser(UserRoleAssignment.of("alice", "r-direct"));
        roleStore.mapGroupToRole(GroupRoleMapping.of("g1", "r-via-group"));

        UserIdentity alice = userWithGroups("alice", "g1");
        Set<String> roles = accessControl.effectiveRoles(alice);

        org.assertj.core.api.Assertions.assertThat(roles)
                .containsExactlyInAnyOrder("r-direct", "r-via-group");
    }

    @Test
    void crossCoordinatorViewAndRevocationAreImmediatelyConsistent() {
        // Simulate two coordinators sharing the same stores (single source of truth).
        DynamicRbacAccessControl coord2 = new DynamicRbacAccessControl(
                grantStore, roleStore,
                new RowLevelSecurityFilter(new EmptyRowPolicyStore()),
                new ColumnMaskingProcessor(new EmptyColumnMaskStore()));

        roleStore.createRole(Role.of("analyst"));
        roleStore.grantRoleToUser(UserRoleAssignment.of("alice", "analyst"));
        grantStore.addGrant(PrivilegeGrant.of(
                "analyst", Privilege.SELECT, "TABLE", "iceberg", "default", "orders", null));

        UserIdentity alice = new UserIdentity("alice", Set.of(), Map.of(), Instant.now());
        assertThatCode(() ->
                coord2.checkCanSelectFromTable(alice, "iceberg", "default", "orders"))
                .doesNotThrowAnyException();
        grantStore.revokeGrants("analyst", Privilege.SELECT, "iceberg", "default", "orders");
        assertThatThrownBy(() ->
                coord2.checkCanSelectFromTable(alice, "iceberg", "default", "orders"))
                .isInstanceOf(AccessDeniedException.class);
        assertThatThrownBy(() ->
                accessControl.checkCanSelectFromTable(alice, "iceberg", "default", "orders"))
                .isInstanceOf(AccessDeniedException.class);
    }

    /** Minimal mutable in-process GrantStore for the role-store semantic tests. */
    private static final class MutableGrantStore implements GrantStore {
        private final List<PrivilegeGrant> grants = new ArrayList<>();

        @Override
        public synchronized List<PrivilegeGrant> listGrants() {
            return List.copyOf(grants);
        }

        @Override
        public synchronized List<PrivilegeGrant> listGrantsForRole(String roleName) {
            return grants.stream().filter(g -> g.roleName().equals(roleName)).toList();
        }

        @Override
        public synchronized void addGrant(PrivilegeGrant grant) {
            grants.add(grant);
        }

        @Override
        public synchronized boolean revokeGrant(String grantId) {
            return grants.removeIf(g -> grantId != null && grantId.equals(g.id()));
        }

        @Override
        public synchronized int revokeGrants(String roleName, Privilege privilege,
                                             String catalog, String schema, String table) {
            int before = grants.size();
            grants.removeIf(g -> g.roleName().equals(roleName)
                    && g.privilege() == privilege
                    && matches(catalog, g.catalog()) && matches(schema, g.schema()) && matches(table, g.table()));
            return before - grants.size();
        }

        private static boolean matches(String pattern, String value) {
            return pattern == null || "*".equals(pattern) || pattern.equals(value);
        }
    }
}
