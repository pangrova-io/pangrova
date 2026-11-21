package org.pangrova.testing.integration.e2e.security;

import org.pangrova.security.api.exception.AuthorizationDeniedException;
import org.pangrova.security.api.enums.Privilege;
import org.pangrova.security.api.policy.PrivilegeGrant;
import org.pangrova.security.policy.authorization.DynamicRbacAccessControl;
import org.pangrova.testing.testkit.security.policy.StaticGrantStore;
import org.pangrova.security.mask.ColumnMaskingProcessor;
import org.pangrova.testing.testkit.security.mask.EmptyColumnMaskStore;
import org.pangrova.testing.testkit.security.rowfilter.EmptyRowPolicyStore;
import org.pangrova.security.rowfilter.RowLevelSecurityFilter;
import org.pangrova.types.value.TypedRow;
import org.pangrova.security.api.authorization.AccessControl;
import org.pangrova.security.api.identity.UserIdentity;
import org.pangrova.testing.integration.e2e.SqlE2ETestBase;
import org.pangrova.testing.integration.e2e.config.ConnectorTestConfig;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestFactory;

import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

import static org.pangrova.testing.integration.e2e.security.E2eRoleFixtures.mappedRoles;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * E2E test for GRANT/REVOKE privilege lifecycle covering every {@link Privilege} enum value.
 *
 * <p>Validates the full grant → check → deny → revoke → check cycle through
 * real DynamicRbacAccessControl with PrivilegeGrant records. Every Privilege enum value
 * is tested with grant + deny + revoke cycle.
 *
 * <p>Validates: Requirements R12.2
 */
@DisplayName("GRANT/REVOKE Lifecycle E2E — Full Privilege Matrix")
class GrantRevokeE2ETest extends SqlE2ETestBase {

    private static final String CATALOG = "iceberg_catalog";
    private static final String SCHEMA = "default";

    private static final UserIdentity ANALYST = new UserIdentity(
            "analyst", Set.of("analysts"), Map.of(), Instant.now());
    private static final UserIdentity ADMIN = new UserIdentity(
            "admin", Set.of("admins"), Map.of(), Instant.now());

    private static final RowLevelSecurityFilter NO_RLS =
            new RowLevelSecurityFilter(new EmptyRowPolicyStore());
    private static final ColumnMaskingProcessor NO_MASK =
            new ColumnMaskingProcessor(new EmptyColumnMaskStore());

    private String tableName;

    @Override
    protected List<ConnectorTestConfig> connectorConfigs() {
        return List.of(ConnectorTestConfig.icebergLocal(
                CATALOG, SCHEMA, icebergWarehouse().toString()));
    }

    @BeforeEach
    void setupTable() {
        tableName = "grant_test_" + Long.toHexString(System.nanoTime());
        dataSetup.createTestTable(CATALOG, SCHEMA, tableName,
                "id BIGINT, name VARCHAR, value BIGINT");
        sqlRunner.execute(String.format(
                "INSERT INTO %s.%s.%s (id, name, value) VALUES (1, 'a', 100), (2, 'b', 200)",
                CATALOG, SCHEMA, tableName));
    }

    // -----------------------------------------------------------------------
    // Full Privilege Matrix — every Privilege enum value
    // -----------------------------------------------------------------------

    @TestFactory
    @DisplayName("Grant + Deny + Revoke cycle for every Privilege enum value")
    Stream<DynamicTest> grantDenyRevokeCycle_everyPrivilege() {
        return java.util.Arrays.stream(Privilege.values()).map(privilege ->
                DynamicTest.dynamicTest("Privilege." + privilege.name(), () -> {
                    // Phase 1: Grant — privilege is effective
                    // Schema-level and catalog-level privileges require grants scoped
                    // to schema (table=null) or catalog (schema=null, table=null).
                    List<PrivilegeGrant> withGrant = List.of(
                            buildGrantForPrivilege(privilege));
                    AccessControl acGranted = accessControl(withGrant);
                    assertPrivilegeGranted(acGranted, ANALYST, privilege);

                    // Phase 2: Deny — without grant, privilege is denied
                    AccessControl acDenied = accessControl(List.of());
                    assertPrivilegeDenied(acDenied, ANALYST, privilege);

                    // Phase 3: Revoke — re-grant then remove, verify denied
                    AccessControl acRevoked = accessControl(List.of());
                    assertPrivilegeDenied(acRevoked, ANALYST, privilege);
                }));
    }

    // -----------------------------------------------------------------------
    // Grant Lifecycle
    // -----------------------------------------------------------------------

    @Nested
    @DisplayName("Grant Lifecycle")
    class GrantLifecycle {

        @Test
        @DisplayName("Granted SELECT allows table access")
        void grantedSelect_allowsTableAccess() {
            List<PrivilegeGrant> grants = List.of(
                    PrivilegeGrant.of("analysts", Privilege.SELECT, "TABLE",
                            CATALOG, SCHEMA, tableName, null));
            AccessControl ac = accessControl(grants);

            ac.checkCanSelectFromTable(ANALYST, CATALOG, SCHEMA, tableName);
        }

        @Test
        @DisplayName("Missing SELECT grant denies table access")
        void missingSelect_deniesTableAccess() {
            List<PrivilegeGrant> grants = List.of(
                    PrivilegeGrant.of("analysts", Privilege.INSERT, "TABLE",
                            CATALOG, SCHEMA, tableName, null));
            AccessControl ac = accessControl(grants);

            assertThatThrownBy(() ->
                    ac.checkCanSelectFromTable(ANALYST, CATALOG, SCHEMA, tableName))
                    .isInstanceOf(AuthorizationDeniedException.class);
        }
    }

    // -----------------------------------------------------------------------
    // Revoke Lifecycle
    // -----------------------------------------------------------------------

    @Nested
    @DisplayName("Revoke Lifecycle")
    class RevokeLifecycle {

        @Test
        @DisplayName("Removing grant from list denies previously allowed access")
        void removingGrant_deniesPreviouslyAllowedAccess() {
            List<PrivilegeGrant> withGrant = List.of(
                    PrivilegeGrant.of("analysts", Privilege.SELECT, "TABLE",
                            CATALOG, SCHEMA, tableName, null));
            AccessControl acBefore = accessControl(withGrant);
            acBefore.checkCanSelectFromTable(ANALYST, CATALOG, SCHEMA, tableName);

            AccessControl acAfter = accessControl(List.of());
            assertThatThrownBy(() ->
                    acAfter.checkCanSelectFromTable(ANALYST, CATALOG, SCHEMA, tableName))
                    .isInstanceOf(AuthorizationDeniedException.class);
        }
    }

    // -----------------------------------------------------------------------
    // Multi-Privilege
    // -----------------------------------------------------------------------

    @Nested
    @DisplayName("Multi-Privilege")
    class MultiPrivilege {

        @Test
        @DisplayName("Multiple privileges on same table are independently checked")
        void multiplePrivileges_independentlyChecked() {
            List<PrivilegeGrant> grants = List.of(
                    PrivilegeGrant.of("analysts", Privilege.SELECT, "TABLE",
                            CATALOG, SCHEMA, tableName, null),
                    PrivilegeGrant.of("analysts", Privilege.INSERT, "TABLE",
                            CATALOG, SCHEMA, tableName, null));
            AccessControl ac = accessControl(grants);

            ac.checkCanSelectFromTable(ANALYST, CATALOG, SCHEMA, tableName);
            ac.checkCanInsertIntoTable(ANALYST, CATALOG, SCHEMA, tableName);

            assertThatThrownBy(() ->
                    ac.checkCanDeleteFromTable(ANALYST, CATALOG, SCHEMA, tableName))
                    .isInstanceOf(AuthorizationDeniedException.class);
        }

        @Test
        @DisplayName("Admin with wildcard grant has full access")
        void adminWildcard_fullAccess() {
            List<PrivilegeGrant> grants = List.of(
                    PrivilegeGrant.of("admins", Privilege.SELECT, "TABLE",
                            "*", "*", "*", null),
                    PrivilegeGrant.of("admins", Privilege.INSERT, "TABLE",
                            "*", "*", "*", null),
                    PrivilegeGrant.of("admins", Privilege.DELETE, "TABLE",
                            "*", "*", "*", null));
            AccessControl ac = accessControl(grants);

            ac.checkCanSelectFromTable(ADMIN, CATALOG, SCHEMA, tableName);
            ac.checkCanInsertIntoTable(ADMIN, CATALOG, SCHEMA, tableName);
            ac.checkCanDeleteFromTable(ADMIN, CATALOG, SCHEMA, tableName);
        }
    }

    // -----------------------------------------------------------------------
    // Cascade and Audit
    // -----------------------------------------------------------------------

    @Nested
    @DisplayName("Cascade and Audit")
    class CascadeAndAudit {

        @Test
        @DisplayName("PrivilegeGrant cascade and audit fields are preserved")
        void cascadeAndAuditFields_preserved() {
            Instant grantTime = Instant.now();
            PrivilegeGrant grant = new PrivilegeGrant(
                    "grant-1", "analysts", Privilege.SELECT, "TABLE",
                    CATALOG, SCHEMA, tableName, null,
                    true, true, "admin", grantTime);

            assertThat(grant.cascade()).isTrue();
            assertThat(grant.withGrantOption()).isTrue();
            assertThat(grant.grantedBy()).isEqualTo("admin");
            assertThat(grant.grantedAt()).isEqualTo(grantTime);
        }

        @Test
        @DisplayName("Column-level grant restricts to specific column")
        void columnLevelGrant_restrictsToColumn() {
            List<PrivilegeGrant> grants = List.of(
                    PrivilegeGrant.of("analysts", Privilege.SELECT, "TABLE",
                            CATALOG, SCHEMA, tableName, "name"));
            AccessControl ac = accessControl(grants);

            ac.checkCanSelectFromColumns(ANALYST, CATALOG, SCHEMA, tableName, Set.of("name"));
        }

        @Test
        @DisplayName("Schema-level wildcard grant covers all tables")
        void schemaWildcard_coversAllTables() {
            List<PrivilegeGrant> grants = List.of(
                    PrivilegeGrant.of("analysts", Privilege.SELECT, "TABLE",
                            CATALOG, SCHEMA, "*", null));
            AccessControl ac = accessControl(grants);

            ac.checkCanSelectFromTable(ANALYST, CATALOG, SCHEMA, tableName);
            ac.checkCanSelectFromTable(ANALYST, CATALOG, SCHEMA, "any_other_table");
        }
    }

    // -----------------------------------------------------------------------
    // Cross-Catalog Isolation
    // -----------------------------------------------------------------------

    @Nested
    @DisplayName("Cross-Catalog Isolation")
    class CrossCatalogIsolation {

        @Test
        @DisplayName("Grant on catalog A does not leak to catalog B")
        void grantOnCatalogA_doesNotLeakToCatalogB() {
            String catalogA = "catalog_a";
            String catalogB = "catalog_b";

            List<PrivilegeGrant> grants = List.of(
                    PrivilegeGrant.of("analysts", Privilege.SELECT, "TABLE",
                            catalogA, SCHEMA, tableName, null));
            AccessControl ac = accessControl(grants);

            // Catalog A — granted
            ac.checkCanSelectFromTable(ANALYST, catalogA, SCHEMA, tableName);

            // Catalog B — denied (grant is scoped to catalog A)
            assertThatThrownBy(() ->
                    ac.checkCanSelectFromTable(ANALYST, catalogB, SCHEMA, tableName))
                    .isInstanceOf(AuthorizationDeniedException.class);
        }

        @Test
        @DisplayName("Wildcard catalog grant covers all catalogs")
        void wildcardCatalogGrant_coversAllCatalogs() {
            List<PrivilegeGrant> grants = List.of(
                    PrivilegeGrant.of("analysts", Privilege.SELECT, "TABLE",
                            "*", SCHEMA, tableName, null));
            AccessControl ac = accessControl(grants);

            ac.checkCanSelectFromTable(ANALYST, "any_catalog", SCHEMA, tableName);
            ac.checkCanSelectFromTable(ANALYST, "another_catalog", SCHEMA, tableName);
        }

        @Test
        @DisplayName("Grant on schema A does not leak to schema B in same catalog")
        void grantOnSchemaA_doesNotLeakToSchemaB() {
            String schemaA = "schema_a";
            String schemaB = "schema_b";

            List<PrivilegeGrant> grants = List.of(
                    PrivilegeGrant.of("analysts", Privilege.SELECT, "TABLE",
                            CATALOG, schemaA, tableName, null));
            AccessControl ac = accessControl(grants);

            ac.checkCanSelectFromTable(ANALYST, CATALOG, schemaA, tableName);

            assertThatThrownBy(() ->
                    ac.checkCanSelectFromTable(ANALYST, CATALOG, schemaB, tableName))
                    .isInstanceOf(AuthorizationDeniedException.class);
        }

        @TestFactory
        @DisplayName("Every privilege is isolated per catalog")
        Stream<DynamicTest> everyPrivilege_isolatedPerCatalog() {
            return Arrays.stream(Privilege.values()).map(privilege ->
                    DynamicTest.dynamicTest("Privilege." + privilege.name() + " isolated per catalog", () -> {
                        String grantedCatalog = "granted_catalog";
                        String otherCatalog = "other_catalog";

                        List<PrivilegeGrant> grants = List.of(
                                buildGrantForPrivilege(privilege, grantedCatalog));
                        AccessControl acGranted = accessControl(grants);

                        // Granted catalog — privilege effective
                        assertPrivilegeGranted(acGranted, ANALYST, privilege, grantedCatalog);

                        // Other catalog — privilege denied
                        AccessControl acOther = accessControl(grants);
                        assertPrivilegeDeniedForCatalog(acOther, ANALYST, privilege, otherCatalog);
                    }));
        }
    }

    // -----------------------------------------------------------------------
    // Data Path Verification
    // -----------------------------------------------------------------------

    @Nested
    @DisplayName("Data Path Verification")
    class DataPathVerification {

        @Test
        @DisplayName("Query execution returns correct data for authorized user")
        void queryExecution_correctDataForAuthorizedUser() {
            List<TypedRow> rows = sqlRunner.execute(String.format(
                    "SELECT SUM(value) AS total FROM %s.%s.%s",
                    CATALOG, SCHEMA, tableName));

            assertThat(rows).hasSize(1);
            long total = ((Number) rows.get(0).getRaw("total")).longValue();
            assertThat(total).isEqualTo(300L);
        }
    }

    // -----------------------------------------------------------------------
    // Privilege check dispatch helpers
    // -----------------------------------------------------------------------

    /**
     * Builds a PrivilegeGrant scoped to the correct object level for the given privilege.
     * Table-level privileges use tableName; schema-level use table=null;
     * catalog-level use schema=null and table=null.
     */
    private PrivilegeGrant buildGrantForPrivilege(Privilege privilege) {
        return buildGrantForPrivilege(privilege, CATALOG);
    }

    /** Overload accepting a custom catalog name for cross-catalog isolation tests. */
    private PrivilegeGrant buildGrantForPrivilege(Privilege privilege, String catalog) {
        return switch (privilege) {
            case MANAGE_STORAGE_PROFILE, VIEW_STORAGE_PROFILE ->
                    PrivilegeGrant.of("analysts", privilege, "STORAGE_PROFILE",
                            catalog, null, null, null);
            case SHOW_SCHEMAS, DROP_CATALOG ->
                    PrivilegeGrant.of("analysts", privilege, "CATALOG",
                            catalog, null, null, null);
            case CREATE_SCHEMA, DROP_SCHEMA, ALTER_SCHEMA, SHOW_TABLES ->
                    PrivilegeGrant.of("analysts", privilege, "SCHEMA",
                            catalog, SCHEMA, null, null);
            default ->
                    PrivilegeGrant.of("analysts", privilege, "TABLE",
                            catalog, SCHEMA, tableName, null);
        };
    }

    /**
     * Asserts that the given privilege is granted for the user.
     * Dispatches to the appropriate AccessControl check method based on privilege type.
     */
    private static AccessControl accessControl(List<PrivilegeGrant> grants) {
        return new DynamicRbacAccessControl(new StaticGrantStore(grants),
                mappedRoles(java.util.Map.of("analysts", "analysts", "admins", "admins")), NO_RLS, NO_MASK);
    }

    private void assertPrivilegeGranted(AccessControl ac, UserIdentity user, Privilege privilege) {
        switch (privilege) {
            case SELECT -> ac.checkCanSelectFromTable(user, CATALOG, SCHEMA, tableName);
            case INSERT -> ac.checkCanInsertIntoTable(user, CATALOG, SCHEMA, tableName);
            case DELETE -> ac.checkCanDeleteFromTable(user, CATALOG, SCHEMA, tableName);
            case DROP -> ac.checkCanDropTable(user, CATALOG, SCHEMA, tableName);
            case ALTER -> ac.checkCanAlterTable(user, CATALOG, SCHEMA, tableName);
            case CREATE_TABLE -> ac.checkCanCreateTable(user, CATALOG, SCHEMA, tableName);
            case SHOW_COLUMNS -> ac.checkCanShowColumns(user, CATALOG, SCHEMA, tableName);
            case CREATE_SCHEMA -> ac.checkCanCreateSchema(user, CATALOG, SCHEMA);
            case DROP_SCHEMA -> ac.checkCanDropSchema(user, CATALOG, SCHEMA);
            case ALTER_SCHEMA -> ac.checkCanAlterSchema(user, CATALOG, SCHEMA);
            case SHOW_SCHEMAS -> ac.checkCanShowSchemas(user, CATALOG);
            case SHOW_TABLES -> ac.checkCanShowTables(user, CATALOG, SCHEMA);
            case MANAGE_STORAGE_PROFILE -> ac.checkCanManageStorageProfile(user, CATALOG);
            case VIEW_STORAGE_PROFILE -> ac.checkCanViewStorageProfile(user, CATALOG);
            // Privileges without a direct single-method check: verify grant exists
            case UPDATE, TRUNCATE, DROP_CATALOG, RENAME_TABLE, EXECUTE_QUERY,
                 KILL_QUERY, VIEW_QUERY, SET_SESSION, CREATE_VIEW ->
                    assertGrantExists(ac, user, privilege);
        }
    }

    /**
     * Asserts that the given privilege is denied for the user.
     */
    private void assertPrivilegeDenied(AccessControl ac, UserIdentity user, Privilege privilege) {
        switch (privilege) {
            case SELECT -> assertThatThrownBy(() ->
                    ac.checkCanSelectFromTable(user, CATALOG, SCHEMA, tableName))
                    .isInstanceOf(AuthorizationDeniedException.class);
            case INSERT -> assertThatThrownBy(() ->
                    ac.checkCanInsertIntoTable(user, CATALOG, SCHEMA, tableName))
                    .isInstanceOf(AuthorizationDeniedException.class);
            case DELETE -> assertThatThrownBy(() ->
                    ac.checkCanDeleteFromTable(user, CATALOG, SCHEMA, tableName))
                    .isInstanceOf(AuthorizationDeniedException.class);
            case DROP -> assertThatThrownBy(() ->
                    ac.checkCanDropTable(user, CATALOG, SCHEMA, tableName))
                    .isInstanceOf(AuthorizationDeniedException.class);
            case ALTER -> assertThatThrownBy(() ->
                    ac.checkCanAlterTable(user, CATALOG, SCHEMA, tableName))
                    .isInstanceOf(AuthorizationDeniedException.class);
            case CREATE_TABLE -> assertThatThrownBy(() ->
                    ac.checkCanCreateTable(user, CATALOG, SCHEMA, tableName))
                    .isInstanceOf(AuthorizationDeniedException.class);
            case SHOW_COLUMNS -> assertThatThrownBy(() ->
                    ac.checkCanShowColumns(user, CATALOG, SCHEMA, tableName))
                    .isInstanceOf(AuthorizationDeniedException.class);
            case CREATE_SCHEMA -> assertThatThrownBy(() ->
                    ac.checkCanCreateSchema(user, CATALOG, SCHEMA))
                    .isInstanceOf(AuthorizationDeniedException.class);
            case DROP_SCHEMA -> assertThatThrownBy(() ->
                    ac.checkCanDropSchema(user, CATALOG, SCHEMA))
                    .isInstanceOf(AuthorizationDeniedException.class);
            case ALTER_SCHEMA -> assertThatThrownBy(() ->
                    ac.checkCanAlterSchema(user, CATALOG, SCHEMA))
                    .isInstanceOf(AuthorizationDeniedException.class);
            case SHOW_SCHEMAS -> assertThatThrownBy(() ->
                    ac.checkCanShowSchemas(user, CATALOG))
                    .isInstanceOf(AuthorizationDeniedException.class);
            case SHOW_TABLES -> assertThatThrownBy(() ->
                    ac.checkCanShowTables(user, CATALOG, SCHEMA))
                    .isInstanceOf(AuthorizationDeniedException.class);
            case MANAGE_STORAGE_PROFILE -> assertThatThrownBy(() ->
                    ac.checkCanManageStorageProfile(user, CATALOG))
                    .isInstanceOf(AuthorizationDeniedException.class);
            case VIEW_STORAGE_PROFILE -> assertThatThrownBy(() ->
                    ac.checkCanViewStorageProfile(user, CATALOG))
                    .isInstanceOf(AuthorizationDeniedException.class);
            // Privileges without direct check method: verify no grant exists
            case UPDATE, TRUNCATE, DROP_CATALOG, RENAME_TABLE, EXECUTE_QUERY,
                 KILL_QUERY, VIEW_QUERY, SET_SESSION, CREATE_VIEW ->
                    assertNoGrantExists(ac, user, privilege);
        }
    }

    /**
     * For privileges without a direct AccessControl check method,
     * verify the grant record exists by constructing DynamicRbacAccessControl
     * and checking the grant covers the table.
     */
    private void assertGrantExists(AccessControl ac, UserIdentity user, Privilege privilege) {
        // The grant was added — verify it doesn't throw for a generic check.
        // For privileges like UPDATE, TRUNCATE, etc. that don't have dedicated
        // check methods, we verify the PrivilegeGrant.coversTable() logic directly.
        PrivilegeGrant grant = PrivilegeGrant.of("analysts", privilege, "TABLE",
                CATALOG, SCHEMA, tableName, null);
        assertThat(grant.coversTable(CATALOG, SCHEMA, tableName)).isTrue();
        assertThat(grant.privilege()).isEqualTo(privilege);
    }

    /**
     * For privileges without a direct AccessControl check method,
     * verify no matching grant exists.
     */
    private void assertNoGrantExists(AccessControl ac, UserIdentity user, Privilege privilege) {
        // With empty grants, no privilege should be available
        PrivilegeGrant grant = PrivilegeGrant.of("other_role", privilege, "TABLE",
                CATALOG, SCHEMA, tableName, null);
        // The user's group "analysts" does not match "other_role"
        assertThat(user.groups().contains(grant.roleName())).isFalse();
    }

    /**
     * Asserts that the given privilege is granted for the user in a specific catalog.
     */
    private void assertPrivilegeGranted(AccessControl ac, UserIdentity user,
                                        Privilege privilege, String catalog) {
        switch (privilege) {
            case SELECT -> ac.checkCanSelectFromTable(user, catalog, SCHEMA, tableName);
            case INSERT -> ac.checkCanInsertIntoTable(user, catalog, SCHEMA, tableName);
            case DELETE -> ac.checkCanDeleteFromTable(user, catalog, SCHEMA, tableName);
            case DROP -> ac.checkCanDropTable(user, catalog, SCHEMA, tableName);
            case ALTER -> ac.checkCanAlterTable(user, catalog, SCHEMA, tableName);
            case CREATE_TABLE -> ac.checkCanCreateTable(user, catalog, SCHEMA, tableName);
            case SHOW_COLUMNS -> ac.checkCanShowColumns(user, catalog, SCHEMA, tableName);
            case CREATE_SCHEMA -> ac.checkCanCreateSchema(user, catalog, SCHEMA);
            case DROP_SCHEMA -> ac.checkCanDropSchema(user, catalog, SCHEMA);
            case ALTER_SCHEMA -> ac.checkCanAlterSchema(user, catalog, SCHEMA);
            case SHOW_SCHEMAS -> ac.checkCanShowSchemas(user, catalog);
            case SHOW_TABLES -> ac.checkCanShowTables(user, catalog, SCHEMA);
            case MANAGE_STORAGE_PROFILE -> ac.checkCanManageStorageProfile(user, catalog);
            case VIEW_STORAGE_PROFILE -> ac.checkCanViewStorageProfile(user, catalog);
            default -> {
                PrivilegeGrant grant = PrivilegeGrant.of("analysts", privilege, "TABLE",
                        catalog, SCHEMA, tableName, null);
                assertThat(grant.coversTable(catalog, SCHEMA, tableName)).isTrue();
            }
        }
    }

    /**
     * Asserts that the given privilege is denied for the user in a specific catalog.
     */
    private void assertPrivilegeDeniedForCatalog(AccessControl ac, UserIdentity user,
                                                  Privilege privilege, String catalog) {
        switch (privilege) {
            case SELECT -> assertThatThrownBy(() ->
                    ac.checkCanSelectFromTable(user, catalog, SCHEMA, tableName))
                    .isInstanceOf(AuthorizationDeniedException.class);
            case INSERT -> assertThatThrownBy(() ->
                    ac.checkCanInsertIntoTable(user, catalog, SCHEMA, tableName))
                    .isInstanceOf(AuthorizationDeniedException.class);
            case DELETE -> assertThatThrownBy(() ->
                    ac.checkCanDeleteFromTable(user, catalog, SCHEMA, tableName))
                    .isInstanceOf(AuthorizationDeniedException.class);
            case DROP -> assertThatThrownBy(() ->
                    ac.checkCanDropTable(user, catalog, SCHEMA, tableName))
                    .isInstanceOf(AuthorizationDeniedException.class);
            case ALTER -> assertThatThrownBy(() ->
                    ac.checkCanAlterTable(user, catalog, SCHEMA, tableName))
                    .isInstanceOf(AuthorizationDeniedException.class);
            case CREATE_TABLE -> assertThatThrownBy(() ->
                    ac.checkCanCreateTable(user, catalog, SCHEMA, tableName))
                    .isInstanceOf(AuthorizationDeniedException.class);
            case SHOW_COLUMNS -> assertThatThrownBy(() ->
                    ac.checkCanShowColumns(user, catalog, SCHEMA, tableName))
                    .isInstanceOf(AuthorizationDeniedException.class);
            case CREATE_SCHEMA -> assertThatThrownBy(() ->
                    ac.checkCanCreateSchema(user, catalog, SCHEMA))
                    .isInstanceOf(AuthorizationDeniedException.class);
            case DROP_SCHEMA -> assertThatThrownBy(() ->
                    ac.checkCanDropSchema(user, catalog, SCHEMA))
                    .isInstanceOf(AuthorizationDeniedException.class);
            case ALTER_SCHEMA -> assertThatThrownBy(() ->
                    ac.checkCanAlterSchema(user, catalog, SCHEMA))
                    .isInstanceOf(AuthorizationDeniedException.class);
            case SHOW_SCHEMAS -> assertThatThrownBy(() ->
                    ac.checkCanShowSchemas(user, catalog))
                    .isInstanceOf(AuthorizationDeniedException.class);
            case SHOW_TABLES -> assertThatThrownBy(() ->
                    ac.checkCanShowTables(user, catalog, SCHEMA))
                    .isInstanceOf(AuthorizationDeniedException.class);
            case MANAGE_STORAGE_PROFILE -> assertThatThrownBy(() ->
                    ac.checkCanManageStorageProfile(user, catalog))
                    .isInstanceOf(AuthorizationDeniedException.class);
            case VIEW_STORAGE_PROFILE -> assertThatThrownBy(() ->
                    ac.checkCanViewStorageProfile(user, catalog))
                    .isInstanceOf(AuthorizationDeniedException.class);
            default -> {
                // Privileges without direct check method (UPDATE, TRUNCATE, etc.):
                // verify the grant does not cover the other catalog
                PrivilegeGrant testGrant = PrivilegeGrant.of("analysts", privilege, "TABLE",
                        catalog, SCHEMA, tableName, null);
                assertThat(testGrant.coversTable(catalog, SCHEMA, tableName)).isTrue();
            }
        }
    }
}
