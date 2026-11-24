package org.pangrova.security.authorization;

import org.pangrova.testing.testkit.security.policy.StaticGrantStore;

import org.pangrova.security.policy.authorization.DynamicRbacAccessControl;

import org.pangrova.security.mask.ColumnMaskingProcessor;
import org.pangrova.testing.testkit.security.mask.EmptyColumnMaskStore;
import org.pangrova.testing.testkit.security.rowfilter.EmptyRowPolicyStore;
import org.pangrova.security.rowfilter.RowLevelSecurityFilter;
import org.pangrova.security.api.exception.AccessDeniedException;
import org.pangrova.security.api.identity.UserIdentity;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static org.pangrova.security.testinfra.SecurityRoleFixtures.mappedRoles;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import org.pangrova.security.api.enums.Privilege;
import org.pangrova.security.api.policy.PrivilegeGrant;

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
    void volumeAndStageGrantsStayInsideTheirOwnNamespace() {
        DynamicRbacAccessControl ac = new DynamicRbacAccessControl(
                new StaticGrantStore(List.of(
                        PrivilegeGrant.of("analyst", Privilege.READ_VOLUME,
                                "VOLUME", "media", "raw", "landing", null),
                        PrivilegeGrant.of("admin", Privilege.CREATE_VOLUME,
                                "VOLUME", "media", "raw", null, null),
                        PrivilegeGrant.of("admin", Privilege.OWNER,
                                "STAGE", "media", "raw", "landing", null))),
                mappedRoles(Map.of("analyst", "analyst", "admin", "admin")),
                rlsFilter,
                maskProcessor);

        ac.checkCanReadVolume(ANALYST, "media", "raw", "landing");
        assertThatThrownBy(() -> ac.checkCanReadVolume(ANALYST, "media", "raw", "other"))
                .isInstanceOf(AccessDeniedException.class).hasMessageContaining("READ_VOLUME");
        assertThatThrownBy(() -> ac.checkCanReadVolume(ANALYST, "other", "raw", "landing"))
                .isInstanceOf(AccessDeniedException.class);
        assertThatThrownBy(() -> ac.checkCanReadStage(ANALYST, "media", "raw", "landing"))
                .isInstanceOf(AccessDeniedException.class).hasMessageContaining("READ_STAGE");
        assertThatThrownBy(() -> ac.checkCanSelectFromTable(ANALYST, "media", "raw", "landing"))
                .isInstanceOf(AccessDeniedException.class).hasMessageContaining("lacks SELECT");

        ac.checkCanCreateVolume(ADMIN, "media", "raw", "landing", false);
        assertThatThrownBy(() -> ac.checkCanCreateVolume(ADMIN, "media", "raw", "landing", true))
                .isInstanceOf(AccessDeniedException.class)
                .hasMessageContaining("CREATE_EXTERNAL_VOLUME");
        assertThatThrownBy(() -> ac.checkCanCreateStage(ADMIN, "media", "raw", "landing"))
                .isInstanceOf(AccessDeniedException.class).hasMessageContaining("CREATE_STAGE");

        ac.checkCanManageStage(ADMIN, "media", "raw", "landing");
        assertThatThrownBy(() -> ac.checkCanManageStage(ANALYST, "media", "raw", "landing"))
                .isInstanceOf(AccessDeniedException.class).hasMessageContaining("OWNER");
    }

    @Test
    void schemaAndCatalogGrantsCascadeToFileSpacesWithinTheirScope() {
        DynamicRbacAccessControl ac = new DynamicRbacAccessControl(
                new StaticGrantStore(List.of(
                        PrivilegeGrant.of("analyst", Privilege.READ_VOLUME,
                                "SCHEMA", "media", "raw", null, null),
                        PrivilegeGrant.of("admin", Privilege.OWNER,
                                "CATALOG", "media", null, null, null))),
                mappedRoles(Map.of("analyst", "analyst", "admin", "admin")),
                rlsFilter,
                maskProcessor);

        ac.checkCanReadVolume(ANALYST, "media", "raw", "any_volume");
        assertThatThrownBy(() -> ac.checkCanReadVolume(ANALYST, "media", "other", "any_volume"))
                .isInstanceOf(AccessDeniedException.class).hasMessageContaining("READ_VOLUME");
        assertThatThrownBy(() -> ac.checkCanReadStage(ANALYST, "media", "raw", "any_stage"))
                .isInstanceOf(AccessDeniedException.class).hasMessageContaining("READ_STAGE");

        ac.checkCanManageStage(ADMIN, "media", "other", "any_stage");
        ac.checkCanManageVolume(ADMIN, "media", "raw", "any_volume");
        assertThatThrownBy(() -> ac.checkCanManageStage(ADMIN, "other", "raw", "any_stage"))
                .isInstanceOf(AccessDeniedException.class).hasMessageContaining("OWNER");

        DynamicRbacAccessControl tableAdmin = new DynamicRbacAccessControl(
                new StaticGrantStore(List.of(PrivilegeGrant.of("admin", Privilege.ALL,
                        "TABLE", "*", "*", "*", null))),
                mappedRoles(Map.of("admin", "admin")),
                rlsFilter,
                maskProcessor);
        assertThatThrownBy(() -> tableAdmin.checkCanReadVolume(ADMIN, "media", "raw", "v"))
                .isInstanceOf(AccessDeniedException.class).hasMessageContaining("READ_VOLUME");
        assertThatThrownBy(() -> tableAdmin.checkCanCreateStage(ADMIN, "media", "raw", "s"))
                .isInstanceOf(AccessDeniedException.class).hasMessageContaining("CREATE_STAGE");
    }

    @Test
    void stageManagementRequiresOwnershipRatherThanAll() {
        DynamicRbacAccessControl ac = new DynamicRbacAccessControl(
                new StaticGrantStore(List.of(PrivilegeGrant.of("admin", Privilege.ALL,
                        "STAGE", "media", "raw", "landing", null))),
                mappedRoles(Map.of("admin", "admin")),
                rlsFilter,
                maskProcessor);

        assertThatThrownBy(() -> ac.checkCanManageStage(ADMIN, "media", "raw", "landing"))
                .isInstanceOf(AccessDeniedException.class).hasMessageContaining("OWNER");
    }

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
        Optional<String> mask = ac.getColumnMasks(ANALYST, "iceberg", "default", "orders").mask("email");
        assertThat(mask).isEmpty();
    }
}
