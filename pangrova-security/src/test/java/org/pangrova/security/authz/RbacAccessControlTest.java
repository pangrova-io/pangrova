package org.pangrova.security.authz;

import org.pangrova.security.mask.ColumnMaskingProcessor;
import org.pangrova.security.mask.EmptyColumnMaskStore;
import org.pangrova.security.rowfilter.EmptyRowPolicyStore;
import org.pangrova.security.rowfilter.RowLevelSecurityFilter;
import org.pangrova.spi.security.AccessDeniedException;
import org.pangrova.spi.security.UserIdentity;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static org.pangrova.security.testinfra.SecurityRoleFixtures.mappedRoles;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Validates: Requirements 18.2 — RBAC access control resolves privileges by role grants.
 */
class RbacAccessControlTest {

    private static final UserIdentity ANALYST = new UserIdentity(
            "alice", Set.of("analyst"), Map.of(), Instant.now());
    private static final UserIdentity ADMIN = new UserIdentity(
            "bob", Set.of("admin"), Map.of(), Instant.now());

    private final RowLevelSecurityFilter rlsFilter =
            new RowLevelSecurityFilter(new EmptyRowPolicyStore());
    private final ColumnMaskingProcessor maskProcessor =
            new ColumnMaskingProcessor(new EmptyColumnMaskStore());

    @Test
    void grantsSelectOnMatchingTable() {
        PrivilegeGrant grant = PrivilegeGrant.of(
                "analyst", Privilege.SELECT, "TABLE", "iceberg", "default", "orders", null);
        DynamicRbacAccessControl ac = new DynamicRbacAccessControl(
                new StaticGrantStore(List.of(grant)),
                mappedRoles(java.util.Map.ofEntries(java.util.Map.entry("analyst", "analyst"), java.util.Map.entry("admin", "admin"))),
                rlsFilter,
                maskProcessor);

        // Should not throw
        ac.checkCanSelectFromTable(ANALYST, "iceberg", "default", "orders");
    }

    @Test
    void deniesSelectOnMissingGrant() {
        DynamicRbacAccessControl ac = new DynamicRbacAccessControl(
                new StaticGrantStore(List.of()),
                mappedRoles(java.util.Map.ofEntries(java.util.Map.entry("analyst", "analyst"), java.util.Map.entry("admin", "admin"))),
                rlsFilter,
                maskProcessor);

        assertThatThrownBy(() -> ac.checkCanSelectFromTable(ANALYST, "iceberg", "default", "orders"))
                .isInstanceOf(AccessDeniedException.class)
                .hasMessageContaining("lacks SELECT");
    }

    @Test
    void wildcardGrantCoversAllTables() {
        PrivilegeGrant grant = PrivilegeGrant.of(
                "admin", Privilege.SELECT, "TABLE", "*", "*", "*", null);
        DynamicRbacAccessControl ac = new DynamicRbacAccessControl(
                new StaticGrantStore(List.of(grant)),
                mappedRoles(java.util.Map.ofEntries(java.util.Map.entry("analyst", "analyst"), java.util.Map.entry("admin", "admin"))),
                rlsFilter,
                maskProcessor);

        ac.checkCanSelectFromTable(ADMIN, "iceberg", "default", "orders");
        ac.checkCanSelectFromTable(ADMIN, "paimon", "analytics", "events");
    }

    @Test
    void checksInsertPrivilegeSeparately() {
        PrivilegeGrant selectGrant = PrivilegeGrant.of(
                "analyst", Privilege.SELECT, "TABLE", "iceberg", "default", "orders", null);
        DynamicRbacAccessControl ac = new DynamicRbacAccessControl(
                new StaticGrantStore(List.of(selectGrant)),
                mappedRoles(java.util.Map.ofEntries(java.util.Map.entry("analyst", "analyst"), java.util.Map.entry("admin", "admin"))),
                rlsFilter,
                maskProcessor);

        assertThatThrownBy(() -> ac.checkCanInsertIntoTable(ANALYST, "iceberg", "default", "orders"))
                .isInstanceOf(AccessDeniedException.class)
                .hasMessageContaining("lacks INSERT");
    }

    @Test
    void checksColumnLevelSelect() {
        // Column grant with column=null acts as table-level in coversTable check
        // To test column-level denial, we need no table-level grant at all
        DynamicRbacAccessControl ac = new DynamicRbacAccessControl(
                new StaticGrantStore(List.of()),
                mappedRoles(java.util.Map.ofEntries(java.util.Map.entry("analyst", "analyst"), java.util.Map.entry("admin", "admin"))),
                rlsFilter,
                maskProcessor);

        assertThatThrownBy(() ->
                ac.checkCanSelectFromColumns(ANALYST, "iceberg", "default", "orders", Set.of("secret")))
                .isInstanceOf(AccessDeniedException.class);
    }

    @Test
    void tableLevelSelectCoversAllColumns() {
        PrivilegeGrant tableGrant = PrivilegeGrant.of(
                "analyst", Privilege.SELECT, "TABLE", "iceberg", "default", "orders", null);
        DynamicRbacAccessControl ac = new DynamicRbacAccessControl(
                new StaticGrantStore(List.of(tableGrant)),
                mappedRoles(java.util.Map.ofEntries(java.util.Map.entry("analyst", "analyst"), java.util.Map.entry("admin", "admin"))),
                rlsFilter,
                maskProcessor);

        ac.checkCanSelectFromColumns(ANALYST, "iceberg", "default", "orders", Set.of("id", "amount", "secret"));
    }

    @Test
    void returnsEmptyRowFilterWhenNoPolicies() {
        DynamicRbacAccessControl ac = new DynamicRbacAccessControl(
                new StaticGrantStore(List.of()),
                mappedRoles(java.util.Map.ofEntries(java.util.Map.entry("analyst", "analyst"), java.util.Map.entry("admin", "admin"))),
                rlsFilter,
                maskProcessor);
        Optional<String> filter = ac.getRowFilter(ANALYST, "iceberg", "default", "orders");
        assertThat(filter).isEmpty();
    }

    @Test
    void returnsEmptyColumnMaskWhenNoPolicies() {
        DynamicRbacAccessControl ac = new DynamicRbacAccessControl(
                new StaticGrantStore(List.of()),
                mappedRoles(java.util.Map.ofEntries(java.util.Map.entry("analyst", "analyst"), java.util.Map.entry("admin", "admin"))),
                rlsFilter,
                maskProcessor);
        Optional<String> mask = ac.getColumnMask(ANALYST, "iceberg", "default", "orders", "email");
        assertThat(mask).isEmpty();
    }
}
