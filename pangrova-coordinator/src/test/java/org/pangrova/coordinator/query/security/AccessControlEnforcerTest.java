package org.pangrova.coordinator.query.security;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.pangrova.security.authz.AuthorizationDeniedException;
import org.pangrova.security.authz.Privilege;
import org.pangrova.security.authz.PrivilegeGrant;
import org.pangrova.security.authz.RbacAccessControl;
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
        void checkTableAccess_nullIdentity_throwsAccessDenied() {
            AccessControlEnforcer enforcer = enforcer(List.of());

            assertThatThrownBy(() -> enforcer.checkTableAccess(List.of(), null))
                    .isInstanceOf(AccessDeniedException.class)
                    .hasMessageContaining("Anonymous access is not allowed");
        }

        @Test
        void checkCreateTable_nullIdentity_throwsAccessDenied() {
            AccessControlEnforcer enforcer = enforcer(List.of());
            assertThatThrownBy(() -> enforcer.checkCreateTable(null, CATALOG, SCHEMA, TABLE))
                    .isInstanceOf(AccessDeniedException.class);
        }

        @Test
        void checkAlterTable_nullIdentity_throwsAccessDenied() {
            AccessControlEnforcer enforcer = enforcer(List.of());
            assertThatThrownBy(() -> enforcer.checkAlterTable(null, CATALOG, SCHEMA, TABLE))
                    .isInstanceOf(AccessDeniedException.class);
        }

        @Test
        void checkDropTable_nullIdentity_throwsAccessDenied() {
            AccessControlEnforcer enforcer = enforcer(List.of());
            assertThatThrownBy(() -> enforcer.checkDropTable(null, CATALOG, SCHEMA, TABLE))
                    .isInstanceOf(AccessDeniedException.class);
        }

        @Test
        void checkInsertIntoTable_nullIdentity_throwsAccessDenied() {
            AccessControlEnforcer enforcer = enforcer(List.of());
            assertThatThrownBy(() -> enforcer.checkInsertIntoTable(null, CATALOG, SCHEMA, TABLE))
                    .isInstanceOf(AccessDeniedException.class);
        }

        @Test
        void checkDeleteFromTable_nullIdentity_throwsAccessDenied() {
            AccessControlEnforcer enforcer = enforcer(List.of());
            assertThatThrownBy(() -> enforcer.checkDeleteFromTable(null, CATALOG, SCHEMA, TABLE))
                    .isInstanceOf(AccessDeniedException.class);
        }

        @Test
        void checkUpdateTable_nullIdentity_throwsAccessDenied() {
            AccessControlEnforcer enforcer = enforcer(List.of());
            assertThatThrownBy(() -> enforcer.checkUpdateTable(null, CATALOG, SCHEMA, TABLE))
                    .isInstanceOf(AccessDeniedException.class);
        }

        @Test
        void checkSelectFromTable_nullIdentity_throwsAccessDenied() {
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
        void allowed_withInsertGrant() {
            AccessControlEnforcer enforcer = enforcer(List.of(grant(Privilege.INSERT)));
            assertThatCode(() -> enforcer.checkInsertIntoTable(user(), CATALOG, SCHEMA, TABLE))
                    .doesNotThrowAnyException();
        }

        @Test
        void denied_withoutInsertGrant() {
            AccessControlEnforcer enforcer = enforcer(List.of(grant(Privilege.SELECT)));
            assertThatThrownBy(() -> enforcer.checkInsertIntoTable(user(), CATALOG, SCHEMA, TABLE))
                    .isInstanceOf(AuthorizationDeniedException.class);
        }
    }

    // ── UPDATE privilege checks ──

    @Nested
    class UpdateAccess {

        @Test
        void allowed_withBothInsertAndDeleteGrants() {
            AccessControlEnforcer enforcer = enforcer(List.of(
                    grant(Privilege.INSERT), grant(Privilege.DELETE)));
            assertThatCode(() -> enforcer.checkUpdateTable(user(), CATALOG, SCHEMA, TABLE))
                    .doesNotThrowAnyException();
        }

        @Test
        void denied_withOnlyInsertGrant() {
            AccessControlEnforcer enforcer = enforcer(List.of(grant(Privilege.INSERT)));
            assertThatThrownBy(() -> enforcer.checkUpdateTable(user(), CATALOG, SCHEMA, TABLE))
                    .isInstanceOf(AuthorizationDeniedException.class);
        }

        @Test
        void denied_withOnlyDeleteGrant() {
            AccessControlEnforcer enforcer = enforcer(List.of(grant(Privilege.DELETE)));
            assertThatThrownBy(() -> enforcer.checkUpdateTable(user(), CATALOG, SCHEMA, TABLE))
                    .isInstanceOf(AuthorizationDeniedException.class);
        }
    }

    // ── DELETE privilege checks ──

    @Nested
    class DeleteAccess {

        @Test
        void allowed_withDeleteGrant() {
            AccessControlEnforcer enforcer = enforcer(List.of(grant(Privilege.DELETE)));
            assertThatCode(() -> enforcer.checkDeleteFromTable(user(), CATALOG, SCHEMA, TABLE))
                    .doesNotThrowAnyException();
        }

        @Test
        void denied_withoutDeleteGrant() {
            AccessControlEnforcer enforcer = enforcer(List.of(grant(Privilege.INSERT)));
            assertThatThrownBy(() -> enforcer.checkDeleteFromTable(user(), CATALOG, SCHEMA, TABLE))
                    .isInstanceOf(AuthorizationDeniedException.class);
        }
    }

    // ── CREATE TABLE privilege checks ──

    @Nested
    class CreateTableAccess {

        @Test
        void allowed_withCreateTableGrant() {
            AccessControlEnforcer enforcer = enforcer(List.of(grant(Privilege.CREATE_TABLE)));
            assertThatCode(() -> enforcer.checkCreateTable(user(), CATALOG, SCHEMA, TABLE))
                    .doesNotThrowAnyException();
        }

        @Test
        void denied_withoutCreateTableGrant() {
            AccessControlEnforcer enforcer = enforcer(List.of(grant(Privilege.SELECT)));
            assertThatThrownBy(() -> enforcer.checkCreateTable(user(), CATALOG, SCHEMA, TABLE))
                    .isInstanceOf(AuthorizationDeniedException.class);
        }
    }

    // ── ALTER TABLE privilege checks ──

    @Nested
    class AlterTableAccess {

        @Test
        void allowed_withAlterGrant() {
            AccessControlEnforcer enforcer = enforcer(List.of(grant(Privilege.ALTER)));
            assertThatCode(() -> enforcer.checkAlterTable(user(), CATALOG, SCHEMA, TABLE))
                    .doesNotThrowAnyException();
        }

        @Test
        void denied_withoutAlterGrant() {
            AccessControlEnforcer enforcer = enforcer(List.of(grant(Privilege.SELECT)));
            assertThatThrownBy(() -> enforcer.checkAlterTable(user(), CATALOG, SCHEMA, TABLE))
                    .isInstanceOf(AuthorizationDeniedException.class);
        }
    }

    // ── DROP TABLE privilege checks ──

    @Nested
    class DropTableAccess {

        @Test
        void allowed_withDropGrant() {
            AccessControlEnforcer enforcer = enforcer(List.of(grant(Privilege.DROP)));
            assertThatCode(() -> enforcer.checkDropTable(user(), CATALOG, SCHEMA, TABLE))
                    .doesNotThrowAnyException();
        }

        @Test
        void denied_withoutDropGrant() {
            AccessControlEnforcer enforcer = enforcer(List.of(grant(Privilege.SELECT)));
            assertThatThrownBy(() -> enforcer.checkDropTable(user(), CATALOG, SCHEMA, TABLE))
                    .isInstanceOf(AuthorizationDeniedException.class);
        }
    }

    // ── Privilege hierarchy: OWNER > ALL > specific ──

    @Nested
    class PrivilegeHierarchy {

        @Test
        void ownerGrant_impliesSelect() {
            AccessControlEnforcer enforcer = enforcer(List.of(grant(Privilege.OWNER)));
            assertThatCode(() -> enforcer.checkTableAccess(List.of(), user()))
                    .doesNotThrowAnyException();
        }

        @Test
        void ownerGrant_impliesInsert() {
            AccessControlEnforcer enforcer = enforcer(List.of(grant(Privilege.OWNER)));
            assertThatCode(() -> enforcer.checkInsertIntoTable(user(), CATALOG, SCHEMA, TABLE))
                    .doesNotThrowAnyException();
        }

        @Test
        void ownerGrant_impliesDelete() {
            AccessControlEnforcer enforcer = enforcer(List.of(grant(Privilege.OWNER)));
            assertThatCode(() -> enforcer.checkDeleteFromTable(user(), CATALOG, SCHEMA, TABLE))
                    .doesNotThrowAnyException();
        }

        @Test
        void ownerGrant_impliesUpdate() {
            AccessControlEnforcer enforcer = enforcer(List.of(grant(Privilege.OWNER)));
            assertThatCode(() -> enforcer.checkUpdateTable(user(), CATALOG, SCHEMA, TABLE))
                    .doesNotThrowAnyException();
        }

        @Test
        void ownerGrant_impliesCreateTable() {
            AccessControlEnforcer enforcer = enforcer(List.of(grant(Privilege.OWNER)));
            assertThatCode(() -> enforcer.checkCreateTable(user(), CATALOG, SCHEMA, TABLE))
                    .doesNotThrowAnyException();
        }

        @Test
        void ownerGrant_impliesAlterTable() {
            AccessControlEnforcer enforcer = enforcer(List.of(grant(Privilege.OWNER)));
            assertThatCode(() -> enforcer.checkAlterTable(user(), CATALOG, SCHEMA, TABLE))
                    .doesNotThrowAnyException();
        }

        @Test
        void ownerGrant_impliesDropTable() {
            AccessControlEnforcer enforcer = enforcer(List.of(grant(Privilege.OWNER)));
            assertThatCode(() -> enforcer.checkDropTable(user(), CATALOG, SCHEMA, TABLE))
                    .doesNotThrowAnyException();
        }

        @Test
        void allGrant_impliesSelect() {
            AccessControlEnforcer enforcer = enforcer(List.of(grant(Privilege.ALL)));
            assertThatCode(() -> enforcer.checkTableAccess(List.of(), user()))
                    .doesNotThrowAnyException();
        }

        @Test
        void allGrant_impliesInsert() {
            AccessControlEnforcer enforcer = enforcer(List.of(grant(Privilege.ALL)));
            assertThatCode(() -> enforcer.checkInsertIntoTable(user(), CATALOG, SCHEMA, TABLE))
                    .doesNotThrowAnyException();
        }

        @Test
        void allGrant_impliesDelete() {
            AccessControlEnforcer enforcer = enforcer(List.of(grant(Privilege.ALL)));
            assertThatCode(() -> enforcer.checkDeleteFromTable(user(), CATALOG, SCHEMA, TABLE))
                    .doesNotThrowAnyException();
        }

        @Test
        void allGrant_impliesUpdate() {
            AccessControlEnforcer enforcer = enforcer(List.of(grant(Privilege.ALL)));
            assertThatCode(() -> enforcer.checkUpdateTable(user(), CATALOG, SCHEMA, TABLE))
                    .doesNotThrowAnyException();
        }

        @Test
        void allGrant_impliesCreateTable() {
            AccessControlEnforcer enforcer = enforcer(List.of(grant(Privilege.ALL)));
            assertThatCode(() -> enforcer.checkCreateTable(user(), CATALOG, SCHEMA, TABLE))
                    .doesNotThrowAnyException();
        }

        @Test
        void allGrant_impliesAlterTable() {
            AccessControlEnforcer enforcer = enforcer(List.of(grant(Privilege.ALL)));
            assertThatCode(() -> enforcer.checkAlterTable(user(), CATALOG, SCHEMA, TABLE))
                    .doesNotThrowAnyException();
        }

        @Test
        void allGrant_impliesDropTable() {
            AccessControlEnforcer enforcer = enforcer(List.of(grant(Privilege.ALL)));
            assertThatCode(() -> enforcer.checkDropTable(user(), CATALOG, SCHEMA, TABLE))
                    .doesNotThrowAnyException();
        }

        @Test
        void specificGrant_doesNotImplyOtherPrivilege() {
            AccessControlEnforcer enforcer = enforcer(List.of(grant(Privilege.SELECT)));
            assertThatThrownBy(() -> enforcer.checkInsertIntoTable(user(), CATALOG, SCHEMA, TABLE))
                    .isInstanceOf(AuthorizationDeniedException.class);
        }
    }

    // ── Plan-level SELECT enforcement (backward compatibility) ──

    @Nested
    class PlanLevelSelectEnforcement {

        @Test
        void constantQueryNoTableScan_passes() {
            AccessControlEnforcer enforcer = enforcer(List.of());
            assertThatCode(() -> enforcer.checkTableAccess(List.of(), user()))
                    .doesNotThrowAnyException();
        }

        @Test
        void selectWithGrant_passes() {
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
        void allowed_withSelectGrant() {
            AccessControlEnforcer enforcer = enforcer(List.of(grant(Privilege.SELECT)));
            assertThatCode(() -> enforcer.checkSelectFromTable(user(), CATALOG, SCHEMA, TABLE))
                    .doesNotThrowAnyException();
        }

        @Test
        void denied_withoutSelectGrant() {
            AccessControlEnforcer enforcer = enforcer(List.of(grant(Privilege.INSERT)));
            assertThatThrownBy(() -> enforcer.checkSelectFromTable(user(), CATALOG, SCHEMA, TABLE))
                    .isInstanceOf(org.pangrova.security.authz.AuthorizationDeniedException.class);
        }

        @Test
        void denied_withNoGrants() {
            AccessControlEnforcer enforcer = enforcer(List.of());
            assertThatThrownBy(() -> enforcer.checkSelectFromTable(user(), CATALOG, SCHEMA, TABLE))
                    .isInstanceOf(org.pangrova.security.authz.AuthorizationDeniedException.class);
        }

        @Test
        void allowed_withOwnerGrant() {
            AccessControlEnforcer enforcer = enforcer(List.of(grant(Privilege.OWNER)));
            assertThatCode(() -> enforcer.checkSelectFromTable(user(), CATALOG, SCHEMA, TABLE))
                    .doesNotThrowAnyException();
        }

        @Test
        void allowed_withAllGrant() {
            AccessControlEnforcer enforcer = enforcer(List.of(grant(Privilege.ALL)));
            assertThatCode(() -> enforcer.checkSelectFromTable(user(), CATALOG, SCHEMA, TABLE))
                    .doesNotThrowAnyException();
        }
    }

    // ── MERGE privilege checks (requires both INSERT and DELETE) ──

    @Nested
    class MergeAccess {

        @Test
        void allowed_withBothInsertAndDeleteGrants() {
            AccessControlEnforcer enforcer = enforcer(List.of(
                    grant(Privilege.INSERT), grant(Privilege.DELETE)));
            assertThatCode(() -> enforcer.checkUpdateTable(user(), CATALOG, SCHEMA, TABLE))
                    .doesNotThrowAnyException();
        }

        @Test
        void denied_withOnlyInsertGrant() {
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
        return new RbacAccessControl(grants,
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
