package org.pangrova.coordinator.query.security;

import static org.pangrova.coordinator.testinfra.CoordinatorRoleFixtures.mappedRoles;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.pangrova.security.authz.AuthorizationDeniedException;
import org.pangrova.security.authz.Privilege;
import org.pangrova.security.authz.PrivilegeGrant;
import org.pangrova.security.authz.DynamicRbacAccessControl;
import org.pangrova.security.authz.StaticGrantStore;
import org.pangrova.security.mask.ColumnMaskingProcessor;
import org.pangrova.security.mask.EmptyColumnMaskStore;
import org.pangrova.security.rowfilter.EmptyRowPolicyStore;
import org.pangrova.security.rowfilter.RowLevelSecurityFilter;
import org.pangrova.spi.security.AccessControl;
import org.pangrova.spi.security.AccessDeniedException;
import org.pangrova.spi.security.UserIdentity;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Tests for AccessControlEnforcer covering all DML and DDL operation types
 * with allowed/denied scenarios and privilege hierarchy validation.
 */
class AccessControlEnforcerTest {

    private static final String CATALOG = "cat";
    private static final String SCHEMA = "sch";
    private static final String TABLE = "tbl";

    // ── Anonymous access rejection ──

    @Nested
    class AnonymousAccess {

        @Test
        void checkTableAccessNullIdentityThrowsAccessDenied() {
            AccessControlEnforcer enforcer = enforcer(List.of());

            assertThatThrownBy(() -> enforcer.checkTableAccess(List.of(), null))
                    .isInstanceOf(AccessDeniedException.class)
                    .hasMessageContaining("Anonymous access is not allowed");
        }

        @Test
        void checkCreateTableNullIdentityThrowsAccessDenied() {
            AccessControlEnforcer enforcer = enforcer(List.of());
            assertThatThrownBy(() -> enforcer.checkCreateTable(null, CATALOG, SCHEMA, TABLE))
                    .isInstanceOf(AccessDeniedException.class);
        }

        @Test
        void checkAlterTableNullIdentityThrowsAccessDenied() {
            AccessControlEnforcer enforcer = enforcer(List.of());
            assertThatThrownBy(() -> enforcer.checkAlterTable(null, CATALOG, SCHEMA, TABLE))
                    .isInstanceOf(AccessDeniedException.class);
        }

        @Test
        void checkDropTableNullIdentityThrowsAccessDenied() {
            AccessControlEnforcer enforcer = enforcer(List.of());
            assertThatThrownBy(() -> enforcer.checkDropTable(null, CATALOG, SCHEMA, TABLE))
                    .isInstanceOf(AccessDeniedException.class);
        }

        @Test
        void checkInsertIntoTableNullIdentityThrowsAccessDenied() {
            AccessControlEnforcer enforcer = enforcer(List.of());
            assertThatThrownBy(() -> enforcer.checkInsertIntoTable(null, CATALOG, SCHEMA, TABLE))
                    .isInstanceOf(AccessDeniedException.class);
        }

        @Test
        void checkDeleteFromTableNullIdentityThrowsAccessDenied() {
            AccessControlEnforcer enforcer = enforcer(List.of());
            assertThatThrownBy(() -> enforcer.checkDeleteFromTable(null, CATALOG, SCHEMA, TABLE))
                    .isInstanceOf(AccessDeniedException.class);
        }

        @Test
        void checkUpdateTableNullIdentityThrowsAccessDenied() {
            AccessControlEnforcer enforcer = enforcer(List.of());
            assertThatThrownBy(() -> enforcer.checkUpdateTable(null, CATALOG, SCHEMA, TABLE))
                    .isInstanceOf(AccessDeniedException.class);
        }

        @Test
        void checkSelectFromTableNullIdentityThrowsAccessDenied() {
            AccessControlEnforcer enforcer = enforcer(List.of());
            assertThatThrownBy(() -> enforcer.checkSelectFromTable(null, CATALOG, SCHEMA, TABLE))
                    .isInstanceOf(AccessDeniedException.class)
                    .hasMessageContaining("Anonymous access is not allowed");
        }
    }

    // ── INSERT privilege checks ──

    @Nested
    class InsertAccess {

        @Test
        void allowedWithInsertGrant() {
            AccessControlEnforcer enforcer = enforcer(List.of(grant(Privilege.INSERT)));
            assertThatCode(() -> enforcer.checkInsertIntoTable(user(), CATALOG, SCHEMA, TABLE))
                    .doesNotThrowAnyException();
        }

        @Test
        void deniedWithoutInsertGrant() {
            AccessControlEnforcer enforcer = enforcer(List.of(grant(Privilege.SELECT)));
            assertThatThrownBy(() -> enforcer.checkInsertIntoTable(user(), CATALOG, SCHEMA, TABLE))
                    .isInstanceOf(AuthorizationDeniedException.class);
        }
    }

    // ── UPDATE privilege checks ──

    @Nested
    class UpdateAccess {

        @Test
        void allowedWithSqlUpdateGrant() {
            AccessControlEnforcer enforcer = enforcer(List.of(grant(Privilege.UPDATE)));
            assertThatCode(() -> enforcer.checkUpdateTable(user(), CATALOG, SCHEMA, TABLE))
                    .doesNotThrowAnyException();
        }

        @Test
        void deniedWithOnlyInsertGrant() {
            AccessControlEnforcer enforcer = enforcer(List.of(grant(Privilege.INSERT)));
            assertThatThrownBy(() -> enforcer.checkUpdateTable(user(), CATALOG, SCHEMA, TABLE))
                    .isInstanceOf(AuthorizationDeniedException.class);
        }

        @Test
        void deniedWithOnlyDeleteGrant() {
            AccessControlEnforcer enforcer = enforcer(List.of(grant(Privilege.DELETE)));
            assertThatThrownBy(() -> enforcer.checkUpdateTable(user(), CATALOG, SCHEMA, TABLE))
                    .isInstanceOf(AuthorizationDeniedException.class);
        }
    }

    // ── DELETE privilege checks ──

    @Nested
    class DeleteAccess {

        @Test
        void allowedWithDeleteGrant() {
            AccessControlEnforcer enforcer = enforcer(List.of(grant(Privilege.DELETE)));
            assertThatCode(() -> enforcer.checkDeleteFromTable(user(), CATALOG, SCHEMA, TABLE))
                    .doesNotThrowAnyException();
        }

        @Test
        void deniedWithoutDeleteGrant() {
            AccessControlEnforcer enforcer = enforcer(List.of(grant(Privilege.INSERT)));
            assertThatThrownBy(() -> enforcer.checkDeleteFromTable(user(), CATALOG, SCHEMA, TABLE))
                    .isInstanceOf(AuthorizationDeniedException.class);
        }
    }

    // ── CREATE TABLE privilege checks ──

    @Nested
    class CreateTableAccess {

        @Test
        void allowedWithCreateTableGrant() {
            AccessControlEnforcer enforcer = enforcer(List.of(grant(Privilege.CREATE_TABLE)));
            assertThatCode(() -> enforcer.checkCreateTable(user(), CATALOG, SCHEMA, TABLE))
                    .doesNotThrowAnyException();
        }

        @Test
        void deniedWithoutCreateTableGrant() {
            AccessControlEnforcer enforcer = enforcer(List.of(grant(Privilege.SELECT)));
            assertThatThrownBy(() -> enforcer.checkCreateTable(user(), CATALOG, SCHEMA, TABLE))
                    .isInstanceOf(AuthorizationDeniedException.class);
        }
    }

    // ── ALTER TABLE privilege checks ──

    @Nested
    class AlterTableAccess {

        @Test
        void allowedWithAlterGrant() {
            AccessControlEnforcer enforcer = enforcer(List.of(grant(Privilege.ALTER)));
            assertThatCode(() -> enforcer.checkAlterTable(user(), CATALOG, SCHEMA, TABLE))
                    .doesNotThrowAnyException();
        }

        @Test
        void deniedWithoutAlterGrant() {
            AccessControlEnforcer enforcer = enforcer(List.of(grant(Privilege.SELECT)));
            assertThatThrownBy(() -> enforcer.checkAlterTable(user(), CATALOG, SCHEMA, TABLE))
                    .isInstanceOf(AuthorizationDeniedException.class);
        }
    }

    // ── DROP TABLE privilege checks ──

    @Nested
    class DropTableAccess {

        @Test
        void allowedWithDropGrant() {
            AccessControlEnforcer enforcer = enforcer(List.of(grant(Privilege.DROP)));
            assertThatCode(() -> enforcer.checkDropTable(user(), CATALOG, SCHEMA, TABLE))
                    .doesNotThrowAnyException();
        }

        @Test
        void deniedWithoutDropGrant() {
            AccessControlEnforcer enforcer = enforcer(List.of(grant(Privilege.SELECT)));
            assertThatThrownBy(() -> enforcer.checkDropTable(user(), CATALOG, SCHEMA, TABLE))
                    .isInstanceOf(AuthorizationDeniedException.class);
        }
    }

    // ── Privilege hierarchy: OWNER > ALL > specific ──

    @Nested
    class PrivilegeHierarchy {

        @Test
        void ownerGrantImpliesSelect() {
            AccessControlEnforcer enforcer = enforcer(List.of(grant(Privilege.OWNER)));
            assertThatCode(() -> enforcer.checkTableAccess(List.of(), user()))
                    .doesNotThrowAnyException();
        }

        @Test
        void ownerGrantImpliesInsert() {
            AccessControlEnforcer enforcer = enforcer(List.of(grant(Privilege.OWNER)));
            assertThatCode(() -> enforcer.checkInsertIntoTable(user(), CATALOG, SCHEMA, TABLE))
                    .doesNotThrowAnyException();
        }

        @Test
        void ownerGrantImpliesDelete() {
            AccessControlEnforcer enforcer = enforcer(List.of(grant(Privilege.OWNER)));
            assertThatCode(() -> enforcer.checkDeleteFromTable(user(), CATALOG, SCHEMA, TABLE))
                    .doesNotThrowAnyException();
        }

        @Test
        void ownerGrantImpliesUpdate() {
            AccessControlEnforcer enforcer = enforcer(List.of(grant(Privilege.OWNER)));
            assertThatCode(() -> enforcer.checkUpdateTable(user(), CATALOG, SCHEMA, TABLE))
                    .doesNotThrowAnyException();
        }

        @Test
        void ownerGrantImpliesCreateTable() {
            AccessControlEnforcer enforcer = enforcer(List.of(grant(Privilege.OWNER)));
            assertThatCode(() -> enforcer.checkCreateTable(user(), CATALOG, SCHEMA, TABLE))
                    .doesNotThrowAnyException();
        }

        @Test
        void ownerGrantImpliesAlterTable() {
            AccessControlEnforcer enforcer = enforcer(List.of(grant(Privilege.OWNER)));
            assertThatCode(() -> enforcer.checkAlterTable(user(), CATALOG, SCHEMA, TABLE))
                    .doesNotThrowAnyException();
        }

        @Test
        void ownerGrantImpliesDropTable() {
            AccessControlEnforcer enforcer = enforcer(List.of(grant(Privilege.OWNER)));
            assertThatCode(() -> enforcer.checkDropTable(user(), CATALOG, SCHEMA, TABLE))
                    .doesNotThrowAnyException();
        }

        @Test
        void allGrantImpliesSelect() {
            AccessControlEnforcer enforcer = enforcer(List.of(grant(Privilege.ALL)));
            assertThatCode(() -> enforcer.checkTableAccess(List.of(), user()))
                    .doesNotThrowAnyException();
        }

        @Test
        void allGrantImpliesInsert() {
            AccessControlEnforcer enforcer = enforcer(List.of(grant(Privilege.ALL)));
            assertThatCode(() -> enforcer.checkInsertIntoTable(user(), CATALOG, SCHEMA, TABLE))
                    .doesNotThrowAnyException();
        }

        @Test
        void allGrantImpliesDelete() {
            AccessControlEnforcer enforcer = enforcer(List.of(grant(Privilege.ALL)));
            assertThatCode(() -> enforcer.checkDeleteFromTable(user(), CATALOG, SCHEMA, TABLE))
                    .doesNotThrowAnyException();
        }

        @Test
        void allGrantImpliesUpdate() {
            AccessControlEnforcer enforcer = enforcer(List.of(grant(Privilege.ALL)));
            assertThatCode(() -> enforcer.checkUpdateTable(user(), CATALOG, SCHEMA, TABLE))
                    .doesNotThrowAnyException();
        }

        @Test
        void allGrantImpliesCreateTable() {
            AccessControlEnforcer enforcer = enforcer(List.of(grant(Privilege.ALL)));
            assertThatCode(() -> enforcer.checkCreateTable(user(), CATALOG, SCHEMA, TABLE))
                    .doesNotThrowAnyException();
        }

        @Test
        void allGrantImpliesAlterTable() {
            AccessControlEnforcer enforcer = enforcer(List.of(grant(Privilege.ALL)));
            assertThatCode(() -> enforcer.checkAlterTable(user(), CATALOG, SCHEMA, TABLE))
                    .doesNotThrowAnyException();
        }

        @Test
        void allGrantImpliesDropTable() {
            AccessControlEnforcer enforcer = enforcer(List.of(grant(Privilege.ALL)));
            assertThatCode(() -> enforcer.checkDropTable(user(), CATALOG, SCHEMA, TABLE))
                    .doesNotThrowAnyException();
        }

        @Test
        void specificGrantDoesNotImplyOtherPrivilege() {
            AccessControlEnforcer enforcer = enforcer(List.of(grant(Privilege.SELECT)));
            assertThatThrownBy(() -> enforcer.checkInsertIntoTable(user(), CATALOG, SCHEMA, TABLE))
                    .isInstanceOf(AuthorizationDeniedException.class);
        }
    }

    // ── Plan-level SELECT enforcement (backward compatibility) ──

    @Nested
    class PlanLevelSelectEnforcement {

        @org.junit.jupiter.params.ParameterizedTest
        @org.junit.jupiter.params.provider.ValueSource(strings = {
                "snapshots", "history", "tags", "options", "schemas", "branches", "manifests", "files", "entries",
                "ENTRIES"})
        void systemTableReadsRequireBaseTableGrant(String suffix) {
            String metadataTable = TABLE + "$" + suffix;
            AccessControlEnforcer baseGranted = enforcer(List.of(grant(Privilege.SELECT)));
            assertThatCode(() -> baseGranted.checkTableAccess(
                    List.of(List.of(CATALOG, SCHEMA, metadataTable)), user()))
                    .doesNotThrowAnyException();
            assertThatCode(() -> baseGranted.checkSelectFromTable(user(), CATALOG, SCHEMA, metadataTable))
                    .doesNotThrowAnyException();
            assertThatCode(() -> baseGranted.checkColumnAccess(
                    CATALOG, SCHEMA, metadataTable, Set.of("record_count"), user()))
                    .doesNotThrowAnyException();

            PrivilegeGrant suffixOnly = PrivilegeGrant.of("role1", Privilege.SELECT,
                    "TABLE", CATALOG, SCHEMA, metadataTable, null);
            AccessControlEnforcer noBaseGrant = enforcer(List.of(suffixOnly));
            assertThatThrownBy(() -> noBaseGrant.checkTableAccess(
                    List.of(List.of(CATALOG, SCHEMA, metadataTable)), user()))
                    .isInstanceOf(AuthorizationDeniedException.class);
            assertThatThrownBy(() -> noBaseGrant.checkSelectFromTable(user(), CATALOG, SCHEMA, metadataTable))
                    .isInstanceOf(AuthorizationDeniedException.class);
            assertThatThrownBy(() -> noBaseGrant.checkColumnAccess(
                    CATALOG, SCHEMA, metadataTable, Set.of("record_count"), user()))
                    .isInstanceOf(AuthorizationDeniedException.class);
        }

        @Test
        void unknownSuffixDoesNotInheritBaseTableGrant() {
            assertThatThrownBy(() -> enforcer(List.of(grant(Privilege.SELECT))).checkTableAccess(
                    List.of(List.of(CATALOG, SCHEMA, TABLE + "$ordinary")), user()))
                    .isInstanceOf(AuthorizationDeniedException.class);
        }

        @Test
        void constantQueryNoTableScanPasses() {
            AccessControlEnforcer enforcer = enforcer(List.of());
            assertThatCode(() -> enforcer.checkTableAccess(List.of(), user()))
                    .doesNotThrowAnyException();
        }

        @Test
        void selectWithGrantPasses() {
            List<PrivilegeGrant> grants = List.of(
                    PrivilegeGrant.of("role1", Privilege.SELECT, "TABLE", "*", "*", "*", null));
            AccessControlEnforcer enforcer = enforcer(grants);
            assertThatCode(() -> enforcer.checkTableAccess(List.of(), user()))
                    .doesNotThrowAnyException();
        }
    }

    // ── Statement-level SELECT privilege checks (CoW source table) ──

    @Nested
    class SelectFromTableAccess {

        @Test
        void allowedWithSelectGrant() {
            AccessControlEnforcer enforcer = enforcer(List.of(grant(Privilege.SELECT)));
            assertThatCode(() -> enforcer.checkSelectFromTable(user(), CATALOG, SCHEMA, TABLE))
                    .doesNotThrowAnyException();
        }

        @Test
        void deniedWithoutSelectGrant() {
            AccessControlEnforcer enforcer = enforcer(List.of(grant(Privilege.INSERT)));
            assertThatThrownBy(() -> enforcer.checkSelectFromTable(user(), CATALOG, SCHEMA, TABLE))
                    .isInstanceOf(org.pangrova.security.authz.AuthorizationDeniedException.class);
        }

        @Test
        void deniedWithNoGrants() {
            AccessControlEnforcer enforcer = enforcer(List.of());
            assertThatThrownBy(() -> enforcer.checkSelectFromTable(user(), CATALOG, SCHEMA, TABLE))
                    .isInstanceOf(org.pangrova.security.authz.AuthorizationDeniedException.class);
        }

        @Test
        void allowedWithOwnerGrant() {
            AccessControlEnforcer enforcer = enforcer(List.of(grant(Privilege.OWNER)));
            assertThatCode(() -> enforcer.checkSelectFromTable(user(), CATALOG, SCHEMA, TABLE))
                    .doesNotThrowAnyException();
        }

        @Test
        void allowedWithAllGrant() {
            AccessControlEnforcer enforcer = enforcer(List.of(grant(Privilege.ALL)));
            assertThatCode(() -> enforcer.checkSelectFromTable(user(), CATALOG, SCHEMA, TABLE))
                    .doesNotThrowAnyException();
        }
    }

    // Physical overwrite privileges cannot substitute for the SQL UPDATE action.

    @Nested
    class UpdateGrantSeparation {

        @Test
        void insertAndDeleteCannotSubstituteForUpdate() {
            AccessControlEnforcer enforcer = enforcer(List.of(
                    grant(Privilege.INSERT), grant(Privilege.DELETE)));
            assertThatThrownBy(() -> enforcer.checkUpdateTable(user(), CATALOG, SCHEMA, TABLE))
                    .isInstanceOf(AuthorizationDeniedException.class);
        }

        @Test
        void deniedWithOnlyInsertGrant() {
            AccessControlEnforcer enforcer = enforcer(List.of(grant(Privilege.INSERT)));
            assertThatThrownBy(() -> enforcer.checkUpdateTable(user(), CATALOG, SCHEMA, TABLE))
                    .isInstanceOf(AuthorizationDeniedException.class);
        }
    }

    // ── Helpers ──

    private static AccessControlEnforcer enforcer(List<PrivilegeGrant> grants) {
        return new AccessControlEnforcer(buildAccessControl(grants));
    }

    private static AccessControl buildAccessControl(List<PrivilegeGrant> grants) {
        return new DynamicRbacAccessControl(
                new StaticGrantStore(grants),
                mappedRoles(java.util.Map.ofEntries(java.util.Map.entry("role1", "role1"))),
                new RowLevelSecurityFilter(new EmptyRowPolicyStore()),
                new ColumnMaskingProcessor(new EmptyColumnMaskStore()));
    }

    private static PrivilegeGrant grant(Privilege privilege) {
        return PrivilegeGrant.of("role1", privilege, "TABLE", CATALOG, SCHEMA, TABLE, null);
    }

    private static UserIdentity user() {
        return new UserIdentity("test-user", Set.of("role1"), Map.of(), Instant.now());
    }
}
