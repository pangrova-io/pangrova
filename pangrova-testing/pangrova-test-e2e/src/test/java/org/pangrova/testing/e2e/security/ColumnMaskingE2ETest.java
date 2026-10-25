package org.pangrova.testing.e2e.security;

import org.pangrova.security.authz.Privilege;
import org.pangrova.security.authz.PrivilegeGrant;
import org.pangrova.security.authz.DynamicRbacAccessControl;
import org.pangrova.security.authz.StaticGrantStore;
import org.pangrova.security.mask.ColumnMask;
import org.pangrova.security.mask.ColumnMaskingProcessor;
import org.pangrova.security.mask.EnrichedColumnMaskStore;
import org.pangrova.security.rowfilter.EmptyRowPolicyStore;
import org.pangrova.security.rowfilter.RowLevelSecurityFilter;
import org.pangrova.spi.connector.data.TypedRow;
import org.pangrova.spi.security.AccessControl;
import org.pangrova.spi.security.UserIdentity;
import org.pangrova.testing.e2e.SqlE2ETestBase;
import org.pangrova.testing.e2e.config.ConnectorTestConfig;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static org.pangrova.testing.e2e.security.E2eRoleFixtures.mappedRoles;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * E2E test for column masking: masked column returns redacted value per user role.
 *
 * <p>Validates that ColumnMaskingProcessor correctly applies mask expressions
 * based on user role membership, and that exempt roles see raw data.
 *
 * <p>Validates: Requirements R12.3
 */
@DisplayName("Column Masking E2E — Redacted Values Per User Role")
class ColumnMaskingE2ETest extends SqlE2ETestBase {

    private static final String CATALOG = "iceberg_catalog";
    private static final String SCHEMA = "default";

    private static final UserIdentity RESTRICTED_USER = new UserIdentity(
            "intern", Set.of("interns"), Map.of(), Instant.now());
    private static final UserIdentity ADMIN_USER = new UserIdentity(
            "admin", Set.of("admins"), Map.of(), Instant.now());
    private static final UserIdentity ANALYST_USER = new UserIdentity(
            "analyst", Set.of("analysts"), Map.of(), Instant.now());

    private static final RowLevelSecurityFilter NO_RLS =
            new RowLevelSecurityFilter(new EmptyRowPolicyStore());

    private String tableName;

    @Override
    protected List<ConnectorTestConfig> connectorConfigs() {
        return List.of(ConnectorTestConfig.icebergLocal(
                CATALOG, SCHEMA, icebergWarehouse().toString()));
    }

    @BeforeEach
    void setupMaskingTestTable() {
        tableName = "mask_test_" + Long.toHexString(System.nanoTime());
        dataSetup.createTestTable(CATALOG, SCHEMA, tableName,
                "id BIGINT, name VARCHAR, salary BIGINT, ssn VARCHAR");
        insertMaskingTestData();
    }

    // -----------------------------------------------------------------------
    // Masked Column Returns Redacted Value for Restricted User
    // -----------------------------------------------------------------------

    @Nested
    @DisplayName("Restricted User Masking")
    class RestrictedUserMasking {

        @Test
        @DisplayName("Masked salary column returns '***' for restricted user")
        void maskedSalary_returnsRedactedForRestrictedUser() {
            ColumnMask salaryMask = new ColumnMask(
                    1L, "salary_mask", CATALOG, SCHEMA, tableName, "salary",
                    "'***'",
                    List.of("interns"),   // applies to interns
                    List.of("admins"),    // admins are exempt
                    true, null);

            EnrichedColumnMaskStore store = buildMaskStore(salaryMask);
            ColumnMaskingProcessor processor = new ColumnMaskingProcessor(store);

            Optional<String> mask = processor.getMask(RESTRICTED_USER, java.util.Set.of("interns"), CATALOG, SCHEMA, tableName, "salary");

            assertThat(mask).isPresent();
            assertThat(mask.get()).isEqualTo("'***'");
        }

        @Test
        @DisplayName("Masked SSN column returns redacted pattern for restricted user")
        void maskedSsn_returnsRedactedPatternForRestrictedUser() {
            ColumnMask ssnMask = new ColumnMask(
                    2L, "ssn_mask", CATALOG, SCHEMA, tableName, "ssn",
                    "'XXX-XX-' || SUBSTRING({column}, 8)",
                    List.of("interns"),
                    List.of("admins"),
                    true, null);

            EnrichedColumnMaskStore store = buildMaskStore(ssnMask);
            ColumnMaskingProcessor processor = new ColumnMaskingProcessor(store);

            Optional<String> mask = processor.getMask(RESTRICTED_USER, java.util.Set.of("interns"), CATALOG, SCHEMA, tableName, "ssn");

            assertThat(mask).isPresent();
            assertThat(mask.get()).isEqualTo("'XXX-XX-' || SUBSTRING(ssn, 8)");
        }
    }

    // -----------------------------------------------------------------------
    // Admin Sees Real Value (Exempt Role)
    // -----------------------------------------------------------------------

    @Nested
    @DisplayName("Admin Exempt From Masking")
    class AdminExemptFromMasking {

        @Test
        @DisplayName("Admin user sees real salary value (exempt from mask)")
        void adminUser_seesRealSalaryValue() {
            ColumnMask salaryMask = new ColumnMask(
                    1L, "salary_mask", CATALOG, SCHEMA, tableName, "salary",
                    "'***'",
                    List.of("interns"),
                    List.of("admins"),
                    true, null);

            EnrichedColumnMaskStore store = buildMaskStore(salaryMask);
            ColumnMaskingProcessor processor = new ColumnMaskingProcessor(store);

            Optional<String> mask = processor.getMask(ADMIN_USER, java.util.Set.of("admins"), CATALOG, SCHEMA, tableName, "salary");

            // Admin is exempt — no mask applied, sees raw data
            assertThat(mask).isEmpty();
        }

        @Test
        @DisplayName("Admin user sees real SSN value (exempt from mask)")
        void adminUser_seesRealSsnValue() {
            ColumnMask ssnMask = new ColumnMask(
                    2L, "ssn_mask", CATALOG, SCHEMA, tableName, "ssn",
                    "'XXX-XX-' || SUBSTRING({column}, 8)",
                    List.of("interns"),
                    List.of("admins"),
                    true, null);

            EnrichedColumnMaskStore store = buildMaskStore(ssnMask);
            ColumnMaskingProcessor processor = new ColumnMaskingProcessor(store);

            Optional<String> mask = processor.getMask(ADMIN_USER, java.util.Set.of("admins"), CATALOG, SCHEMA, tableName, "ssn");

            assertThat(mask).isEmpty();
        }
    }

    // -----------------------------------------------------------------------
    // Per-User Role Masking
    // -----------------------------------------------------------------------

    @Nested
    @DisplayName("Per-User Role Masking")
    class PerUserRoleMasking {

        @Test
        @DisplayName("Mask applies only to targeted role, not to other roles")
        void maskAppliesOnlyToTargetedRole() {
            ColumnMask salaryMask = new ColumnMask(
                    1L, "salary_mask", CATALOG, SCHEMA, tableName, "salary",
                    "'REDACTED'",
                    List.of("interns"),   // only interns
                    List.of(),            // no exemptions
                    true, null);

            EnrichedColumnMaskStore store = buildMaskStore(salaryMask);
            ColumnMaskingProcessor processor = new ColumnMaskingProcessor(store);

            // Intern — masked
            Optional<String> internMask = processor.getMask(RESTRICTED_USER, java.util.Set.of("interns"), CATALOG, SCHEMA, tableName, "salary");
            assertThat(internMask).isPresent();
            assertThat(internMask.get()).isEqualTo("'REDACTED'");

            // Analyst — not in applyToRoles, mask does not apply
            Optional<String> analystMask = processor.getMask(ANALYST_USER, java.util.Set.of("analysts"), CATALOG, SCHEMA, tableName, "salary");
            assertThat(analystMask).isEmpty();
        }

        @Test
        @DisplayName("Universal mask (empty applyToRoles) applies to all non-exempt users")
        void universalMask_appliesToAllNonExemptUsers() {
            ColumnMask universalMask = new ColumnMask(
                    3L, "universal_salary_mask", CATALOG, SCHEMA, tableName, "salary",
                    "0",
                    List.of(),            // applies to everyone
                    List.of("admins"),    // only admins exempt
                    true, null);

            EnrichedColumnMaskStore store = buildMaskStore(universalMask);
            ColumnMaskingProcessor processor = new ColumnMaskingProcessor(store);

            // Intern — masked
            assertThat(processor.getMask(RESTRICTED_USER, java.util.Set.of("interns"), CATALOG, SCHEMA, tableName, "salary"))
                    .isPresent().hasValue("0");

            // Analyst — masked
            assertThat(processor.getMask(ANALYST_USER, java.util.Set.of("analysts"), CATALOG, SCHEMA, tableName, "salary"))
                    .isPresent().hasValue("0");

            // Admin — exempt
            assertThat(processor.getMask(ADMIN_USER, java.util.Set.of("admins"), CATALOG, SCHEMA, tableName, "salary"))
                    .isEmpty();
        }

        @Test
        @DisplayName("Disabled mask is not applied to any user")
        void disabledMask_notAppliedToAnyUser() {
            ColumnMask disabledMask = new ColumnMask(
                    4L, "disabled_mask", CATALOG, SCHEMA, tableName, "salary",
                    "'***'",
                    List.of("interns"),
                    List.of(),
                    false, null);  // disabled

            EnrichedColumnMaskStore store = buildMaskStore(disabledMask);
            ColumnMaskingProcessor processor = new ColumnMaskingProcessor(store);

            Optional<String> mask = processor.getMask(RESTRICTED_USER, java.util.Set.of("interns"), CATALOG, SCHEMA, tableName, "salary");
            assertThat(mask).isEmpty();
        }
    }

    // -----------------------------------------------------------------------
    // AccessControl Integration — getColumnMask via DynamicRbacAccessControl
    // -----------------------------------------------------------------------

    @Nested
    @DisplayName("AccessControl Column Mask Integration")
    class AccessControlIntegration {

        @Test
        @DisplayName("DynamicRbacAccessControl.getColumnMask returns mask for restricted user")
        void rbacAccessControl_returnsMaskForRestrictedUser() {
            ColumnMask salaryMask = new ColumnMask(
                    1L, "salary_mask", CATALOG, SCHEMA, tableName, "salary",
                    "'***'",
                    List.of("interns"),
                    List.of("admins"),
                    true, null);

            EnrichedColumnMaskStore store = buildMaskStore(salaryMask);
            ColumnMaskingProcessor processor = new ColumnMaskingProcessor(store);
            List<PrivilegeGrant> grants = List.of(
                    PrivilegeGrant.of("interns", Privilege.SELECT, "TABLE",
                            CATALOG, SCHEMA, tableName, null));
            AccessControl ac = new DynamicRbacAccessControl(
                new StaticGrantStore(grants),
                mappedRoles(java.util.Map.ofEntries(java.util.Map.entry("interns", "interns"), java.util.Map.entry("admins", "admins"), java.util.Map.entry("analysts", "analysts"))),
                NO_RLS,
                processor);

            Optional<String> mask = ac.getColumnMask(
                    RESTRICTED_USER, CATALOG, SCHEMA, tableName, "salary");
            assertThat(mask).isPresent().hasValue("'***'");
        }

        @Test
        @DisplayName("DynamicRbacAccessControl.getColumnMask returns empty for admin")
        void rbacAccessControl_returnsEmptyForAdmin() {
            ColumnMask salaryMask = new ColumnMask(
                    1L, "salary_mask", CATALOG, SCHEMA, tableName, "salary",
                    "'***'",
                    List.of("interns"),
                    List.of("admins"),
                    true, null);

            EnrichedColumnMaskStore store = buildMaskStore(salaryMask);
            ColumnMaskingProcessor processor = new ColumnMaskingProcessor(store);
            List<PrivilegeGrant> grants = List.of(
                    PrivilegeGrant.of("admins", Privilege.SELECT, "TABLE",
                            CATALOG, SCHEMA, tableName, null));
            AccessControl ac = new DynamicRbacAccessControl(
                new StaticGrantStore(grants),
                mappedRoles(java.util.Map.ofEntries(java.util.Map.entry("interns", "interns"), java.util.Map.entry("admins", "admins"), java.util.Map.entry("analysts", "analysts"))),
                NO_RLS,
                processor);

            Optional<String> mask = ac.getColumnMask(
                    ADMIN_USER, CATALOG, SCHEMA, tableName, "salary");
            assertThat(mask).isEmpty();
        }

        @Test
        @DisplayName("Unmasked column returns empty for all users")
        void unmaskedColumn_returnsEmptyForAllUsers() {
            ColumnMask salaryMask = new ColumnMask(
                    1L, "salary_mask", CATALOG, SCHEMA, tableName, "salary",
                    "'***'",
                    List.of("interns"),
                    List.of("admins"),
                    true, null);

            EnrichedColumnMaskStore store = buildMaskStore(salaryMask);
            ColumnMaskingProcessor processor = new ColumnMaskingProcessor(store);
            List<PrivilegeGrant> grants = List.of(
                    PrivilegeGrant.of("interns", Privilege.SELECT, "TABLE",
                            CATALOG, SCHEMA, tableName, null));
            AccessControl ac = new DynamicRbacAccessControl(
                new StaticGrantStore(grants),
                mappedRoles(java.util.Map.ofEntries(java.util.Map.entry("interns", "interns"), java.util.Map.entry("admins", "admins"), java.util.Map.entry("analysts", "analysts"))),
                NO_RLS,
                processor);

            // "name" column has no mask defined
            Optional<String> mask = ac.getColumnMask(
                    RESTRICTED_USER, CATALOG, SCHEMA, tableName, "name");
            assertThat(mask).isEmpty();
        }
    }

    // -----------------------------------------------------------------------
    // Data Path Verification
    // -----------------------------------------------------------------------

    @Nested
    @DisplayName("Data Path Verification")
    class DataPathVerification {

        @Test
        @DisplayName("Query execution returns correct data for test table")
        void queryExecution_correctDataForTestTable() {
            List<TypedRow> rows = sqlRunner.execute(String.format(
                    "SELECT COUNT(*) AS cnt FROM %s.%s.%s",
                    CATALOG, SCHEMA, tableName));

            assertThat(rows).hasSize(1);
            long count = ((Number) rows.get(0).getRaw("cnt")).longValue();
            assertThat(count).isEqualTo(5L);
        }
    }

    // -----------------------------------------------------------------------
    // Helpers
    // -----------------------------------------------------------------------

    private void insertMaskingTestData() {
        String[] names = {"alice", "bob", "carol", "dave", "eve"};
        long[] salaries = {80000, 90000, 75000, 110000, 95000};
        String[] ssns = {
                "123-45-6789", "234-56-7890", "345-67-8901",
                "456-78-9012", "567-89-0123"
        };
        for (int i = 0; i < names.length; i++) {
            sqlRunner.execute(String.format(
                    "INSERT INTO %s.%s.%s (id, name, salary, ssn) VALUES (%d, '%s', %d, '%s')",
                    CATALOG, SCHEMA, tableName, i + 1, names[i], salaries[i], ssns[i]));
        }
    }

    /**
     * Builds an in-memory EnrichedColumnMaskStore with the given masks.
     */
    private static EnrichedColumnMaskStore buildMaskStore(ColumnMask... masks) {
        List<ColumnMask> maskList = List.of(masks);
        return new EnrichedColumnMaskStore() {
            @Override
            public List<ColumnMask> getTableMasks(String catalog, String schema, String table) {
                return maskList.stream().filter(mask -> mask.catalogName().equals(catalog)
                        && mask.schemaName().equals(schema) && mask.tableName().equals(table)).toList();
            }

            @Override
            public List<ColumnMask> getColumnMasks(
                    String catalog, String schema, String table, String column) {
                return maskList.stream()
                        .filter(m -> m.catalogName().equals(catalog)
                                && m.schemaName().equals(schema)
                                && m.tableName().equals(table)
                                && m.columnName().equals(column))
                        .toList();
            }

            @Override
            public void saveColumnMask(ColumnMask mask) {
                throw new UnsupportedOperationException("read-only test store");
            }

            @Override
            public void deleteMask(long maskId) {
                throw new UnsupportedOperationException("read-only test store");
            }
        };
    }
}
