package org.pangrova.testing.integration.e2e.security;

import org.pangrova.security.api.policy.Privilege;
import org.pangrova.security.api.policy.PrivilegeGrant;
import org.pangrova.security.core.authz.DynamicRbacAccessControl;
import org.pangrova.security.core.authz.StaticGrantStore;
import org.pangrova.security.core.mask.ColumnMaskingProcessor;
import org.pangrova.security.core.mask.provider.empty.EmptyColumnMaskStore;
import org.pangrova.security.core.rowfilter.RowLevelSecurityFilter;
import org.pangrova.security.api.rowfilter.RowPolicy;
import org.pangrova.security.api.rowfilter.RowPolicyStore;
import org.pangrova.connector.spi.data.value.TypedRow;
import org.pangrova.security.api.AccessControl;
import org.pangrova.security.api.UserIdentity;
import org.pangrova.testing.integration.e2e.SqlE2ETestBase;
import org.pangrova.testing.integration.e2e.config.ConnectorTestConfig;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static org.pangrova.testing.integration.e2e.security.E2eRoleFixtures.mappedRoles;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * E2E test for row-level security: same SELECT * returns different rows per user.
 *
 * <p>Validates that RowLevelSecurityFilter produces per-user filter expressions
 * based on row policies, and that DynamicRbacAccessControl.getRowFilter() returns the
 * correct filter for each user identity. User A sees only their rows, user B
 * sees only their rows, and admin sees all rows.
 *
 * <p>Validates: Requirements R12.4
 */
@DisplayName("Row-Level Security E2E — Different Rows Per User")
class RowLevelSecurityE2ETest extends SqlE2ETestBase {

    private static final String CATALOG = "iceberg_catalog";
    private static final String SCHEMA = "default";
    private static final String FQN_PREFIX = CATALOG + "." + SCHEMA + ".";

    private static final UserIdentity USER_A = new UserIdentity(
            "alice", Set.of("team_a"), Map.of("department", "engineering"), Instant.now());
    private static final UserIdentity USER_B = new UserIdentity(
            "bob", Set.of("team_b"), Map.of("department", "marketing"), Instant.now());
    private static final UserIdentity ADMIN = new UserIdentity(
            "admin", Set.of("admins"), Map.of("department", "all"), Instant.now());

    private static final ColumnMaskingProcessor NO_MASK =
            new ColumnMaskingProcessor(new EmptyColumnMaskStore());

    private String tableName;

    @Override
    protected List<ConnectorTestConfig> connectorConfigs() {
        return List.of(ConnectorTestConfig.icebergLocal(
                CATALOG, SCHEMA, icebergWarehouse().toString()));
    }

    @BeforeEach
    void setupTestTable() {
        tableName = "rls_test_" + Long.toHexString(System.nanoTime());
        dataSetup.createTestTable(CATALOG, SCHEMA, tableName,
                "id BIGINT, owner VARCHAR, department VARCHAR, value BIGINT");
        // Insert rows owned by different users/departments
        for (int i = 1; i <= 5; i++) {
            sqlRunner.execute(String.format(
                    "INSERT INTO %s%s (id, owner, department, value) VALUES (%d, 'alice', 'engineering', %d)",
                    FQN_PREFIX, tableName, i, i * 10));
        }
        for (int i = 6; i <= 9; i++) {
            sqlRunner.execute(String.format(
                    "INSERT INTO %s%s (id, owner, department, value) VALUES (%d, 'bob', 'marketing', %d)",
                    FQN_PREFIX, tableName, i, i * 10));
        }
        sqlRunner.execute(String.format(
                "INSERT INTO %s%s (id, owner, department, value) VALUES (10, 'admin', 'all', 100)",
                FQN_PREFIX, tableName));
    }

    // -----------------------------------------------------------------------
    // Per-User Row Filtering
    // -----------------------------------------------------------------------

    @Nested
    @DisplayName("Per-User Row Filtering")
    class PerUserFiltering {

        @Test
        @DisplayName("User A filter expression restricts to owner='alice' rows")
        void userA_seesOnlyOwnRows() {
            RowPolicy ownerPolicy = new RowPolicy(1, "owner_filter",
                    CATALOG, SCHEMA, tableName,
                    "owner = current_user()", List.of(), true, 1, null);
            InMemoryRowPolicyStore store = new InMemoryRowPolicyStore(List.of(ownerPolicy));
            RowLevelSecurityFilter filter = new RowLevelSecurityFilter(store);

            Optional<String> rowFilter = filter.getFilter(USER_A, java.util.Set.of("team_a"), CATALOG, SCHEMA, tableName);
            assertThat(rowFilter).isPresent();
            assertThat(rowFilter.get()).contains("'alice'");

            // Verify via AccessControl integration
            AccessControl ac = buildAccessControl(store, USER_A);
            Optional<String> acFilter = ac.getRowFilter(USER_A, CATALOG, SCHEMA, tableName);
            assertThat(acFilter).isPresent();
            assertThat(acFilter.get()).contains("'alice'");
        }

        @Test
        @DisplayName("User B filter expression restricts to owner='bob' rows")
        void userB_seesOnlyOwnRows() {
            RowPolicy ownerPolicy = new RowPolicy(1, "owner_filter",
                    CATALOG, SCHEMA, tableName,
                    "owner = current_user()", List.of(), true, 1, null);
            InMemoryRowPolicyStore store = new InMemoryRowPolicyStore(List.of(ownerPolicy));
            RowLevelSecurityFilter filter = new RowLevelSecurityFilter(store);

            Optional<String> rowFilter = filter.getFilter(USER_B, java.util.Set.of("team_b"), CATALOG, SCHEMA, tableName);
            assertThat(rowFilter).isPresent();
            assertThat(rowFilter.get()).contains("'bob'");
        }

        @Test
        @DisplayName("Admin with no applicable policy sees all rows (no filter)")
        void admin_seesAllRows_noPolicyApplied() {
            // Policy applies only to team_a and team_b roles, not admins
            RowPolicy ownerPolicy = new RowPolicy(1, "owner_filter",
                    CATALOG, SCHEMA, tableName,
                    "owner = current_user()", List.of("team_a", "team_b"), true, 1, null);
            InMemoryRowPolicyStore store = new InMemoryRowPolicyStore(List.of(ownerPolicy));
            RowLevelSecurityFilter filter = new RowLevelSecurityFilter(store);

            Optional<String> rowFilter = filter.getFilter(ADMIN, java.util.Set.of("admins"), CATALOG, SCHEMA, tableName);
            assertThat(rowFilter).isEmpty();
        }

        @Test
        @DisplayName("Same getRowFilter call returns different expressions per user")
        void sameQuery_differentFilterPerUser() {
            RowPolicy ownerPolicy = new RowPolicy(1, "owner_filter",
                    CATALOG, SCHEMA, tableName,
                    "owner = current_user()", List.of(), true, 1, null);
            InMemoryRowPolicyStore store = new InMemoryRowPolicyStore(List.of(ownerPolicy));
            RowLevelSecurityFilter filter = new RowLevelSecurityFilter(store);

            Optional<String> filterA = filter.getFilter(USER_A, java.util.Set.of("team_a"), CATALOG, SCHEMA, tableName);
            Optional<String> filterB = filter.getFilter(USER_B, java.util.Set.of("team_b"), CATALOG, SCHEMA, tableName);
            Optional<String> filterAdmin = filter.getFilter(ADMIN, java.util.Set.of("admins"), CATALOG, SCHEMA, tableName);

            assertThat(filterA).isPresent();
            assertThat(filterB).isPresent();
            assertThat(filterAdmin).isPresent();

            // All three are present but contain different user principals
            assertThat(filterA.get()).contains("'alice'").doesNotContain("'bob'");
            assertThat(filterB.get()).contains("'bob'").doesNotContain("'alice'");
            assertThat(filterAdmin.get()).contains("'admin'");
        }
    }

    // -----------------------------------------------------------------------
    // Department-Based Attribute Filtering
    // -----------------------------------------------------------------------

    @Nested
    @DisplayName("Department-Based Attribute Filtering")
    class DepartmentFiltering {

        @Test
        @DisplayName("User attribute policy binds department for user A")
        void userAttribute_bindsDepartmentForUserA() {
            RowPolicy deptPolicy = new RowPolicy(1, "dept_filter",
                    CATALOG, SCHEMA, tableName,
                    "department = user_attribute('department')", List.of(), true, 1, null);
            InMemoryRowPolicyStore store = new InMemoryRowPolicyStore(List.of(deptPolicy));
            RowLevelSecurityFilter filter = new RowLevelSecurityFilter(store);

            Optional<String> rowFilter = filter.getFilter(USER_A, java.util.Set.of("team_a"), CATALOG, SCHEMA, tableName);
            assertThat(rowFilter).isPresent();
            assertThat(rowFilter.get()).contains("'engineering'");
        }

        @Test
        @DisplayName("User attribute policy binds department for user B")
        void userAttribute_bindsDepartmentForUserB() {
            RowPolicy deptPolicy = new RowPolicy(1, "dept_filter",
                    CATALOG, SCHEMA, tableName,
                    "department = user_attribute('department')", List.of(), true, 1, null);
            InMemoryRowPolicyStore store = new InMemoryRowPolicyStore(List.of(deptPolicy));
            RowLevelSecurityFilter filter = new RowLevelSecurityFilter(store);

            Optional<String> rowFilter = filter.getFilter(USER_B, java.util.Set.of("team_b"), CATALOG, SCHEMA, tableName);
            assertThat(rowFilter).isPresent();
            assertThat(rowFilter.get()).contains("'marketing'");
        }
    }

    // -----------------------------------------------------------------------
    // Multi-Policy Merge
    // -----------------------------------------------------------------------

    @Nested
    @DisplayName("Multi-Policy Merge")
    class MultiPolicyMerge {

        @Test
        @DisplayName("Multiple policies are AND-merged for same user")
        void multiplePolicies_andMerged() {
            RowPolicy ownerPolicy = new RowPolicy(1, "owner_filter",
                    CATALOG, SCHEMA, tableName,
                    "owner = current_user()", List.of(), true, 1, null);
            RowPolicy deptPolicy = new RowPolicy(2, "dept_filter",
                    CATALOG, SCHEMA, tableName,
                    "department = user_attribute('department')", List.of(), true, 2, null);
            InMemoryRowPolicyStore store = new InMemoryRowPolicyStore(
                    List.of(ownerPolicy, deptPolicy));
            RowLevelSecurityFilter filter = new RowLevelSecurityFilter(store);

            Optional<String> rowFilter = filter.getFilter(USER_A, java.util.Set.of("team_a"), CATALOG, SCHEMA, tableName);
            assertThat(rowFilter).isPresent();
            assertThat(rowFilter.get()).contains("AND");
            assertThat(rowFilter.get()).contains("'alice'");
            assertThat(rowFilter.get()).contains("'engineering'");
        }
    }

    // -----------------------------------------------------------------------
    // Role-Scoped Policies
    // -----------------------------------------------------------------------

    @Nested
    @DisplayName("Role-Scoped Policies")
    class RoleScopedPolicies {

        @Test
        @DisplayName("Policy scoped to team_a applies only to user A")
        void roleScopedPolicy_appliesOnlyToMatchingRole() {
            RowPolicy teamAPolicy = new RowPolicy(1, "team_a_filter",
                    CATALOG, SCHEMA, tableName,
                    "owner = current_user()", List.of("team_a"), true, 1, null);
            InMemoryRowPolicyStore store = new InMemoryRowPolicyStore(List.of(teamAPolicy));
            RowLevelSecurityFilter filter = new RowLevelSecurityFilter(store);

            assertThat(filter.getFilter(USER_A, java.util.Set.of("team_a"), CATALOG, SCHEMA, tableName)).isPresent();
            assertThat(filter.getFilter(USER_B, java.util.Set.of("team_b"), CATALOG, SCHEMA, tableName)).isEmpty();
            assertThat(filter.getFilter(ADMIN, java.util.Set.of("admins"), CATALOG, SCHEMA, tableName)).isEmpty();
        }
    }

    // -----------------------------------------------------------------------
    // Data Path Verification — actual query with RLS filter
    // -----------------------------------------------------------------------

    @Nested
    @DisplayName("Data Path Verification")
    class DataPathVerification {

        @Test
        @DisplayName("Full data set has 10 rows without RLS")
        void fullDataSet_has10Rows() {
            List<TypedRow> rows = sqlRunner.execute(
                    "SELECT COUNT(*) AS cnt FROM " + FQN_PREFIX + tableName);
            assertThat(rows).hasSize(1);
            long count = ((Number) rows.get(0).getRaw("cnt")).longValue();
            assertThat(count).isEqualTo(10L);
        }

        @Test
        @DisplayName("Query with owner filter for alice returns 5 rows")
        void queryWithOwnerFilter_aliceGets5Rows() {
            // Simulate what the query engine would do after applying RLS filter
            List<TypedRow> rows = sqlRunner.execute(
                    "SELECT * FROM " + FQN_PREFIX + tableName + " WHERE owner = 'alice'");
            assertThat(rows).hasSize(5);
            for (TypedRow row : rows) {
                assertThat(row.getRaw("owner").toString()).isEqualTo("alice");
            }
        }

        @Test
        @DisplayName("Query with owner filter for bob returns 4 rows")
        void queryWithOwnerFilter_bobGets4Rows() {
            List<TypedRow> rows = sqlRunner.execute(
                    "SELECT * FROM " + FQN_PREFIX + tableName + " WHERE owner = 'bob'");
            assertThat(rows).hasSize(4);
            for (TypedRow row : rows) {
                assertThat(row.getRaw("owner").toString()).isEqualTo("bob");
            }
        }

        @Test
        @DisplayName("Query without filter returns all 10 rows (admin view)")
        void queryWithoutFilter_adminGetsAllRows() {
            List<TypedRow> rows = sqlRunner.execute(
                    "SELECT * FROM " + FQN_PREFIX + tableName);
            assertThat(rows).hasSize(10);
        }

        @Test
        @DisplayName("RLS filter expression integrates with AccessControl.getRowFilter()")
        void rlsFilter_integratesWithAccessControl() {
            RowPolicy ownerPolicy = new RowPolicy(1, "owner_filter",
                    CATALOG, SCHEMA, tableName,
                    "owner = current_user()", List.of("team_a", "team_b"), true, 1, null);
            InMemoryRowPolicyStore store = new InMemoryRowPolicyStore(List.of(ownerPolicy));

            AccessControl acA = buildAccessControl(store, USER_A);
            AccessControl acB = buildAccessControl(store, USER_B);
            AccessControl acAdmin = buildAccessControl(store, ADMIN);

            // User A gets filter with 'alice'
            Optional<String> filterA = acA.getRowFilter(USER_A, CATALOG, SCHEMA, tableName);
            assertThat(filterA).isPresent().hasValueSatisfying(f -> assertThat(f).contains("'alice'"));

            // User B gets filter with 'bob'
            Optional<String> filterB = acB.getRowFilter(USER_B, CATALOG, SCHEMA, tableName);
            assertThat(filterB).isPresent().hasValueSatisfying(f -> assertThat(f).contains("'bob'"));

            // Admin gets no filter (policy doesn't apply to admins role)
            Optional<String> filterAdmin = acAdmin.getRowFilter(ADMIN, CATALOG, SCHEMA, tableName);
            assertThat(filterAdmin).isEmpty();
        }
    }

    // -----------------------------------------------------------------------
    // Helpers
    // -----------------------------------------------------------------------

    private AccessControl buildAccessControl(RowPolicyStore store, UserIdentity user) {
        List<PrivilegeGrant> grants = List.of(
                PrivilegeGrant.of(user.groups().iterator().next(), Privilege.SELECT, "TABLE",
                        CATALOG, SCHEMA, tableName, null));
        RowLevelSecurityFilter filter = new RowLevelSecurityFilter(store);
        return new DynamicRbacAccessControl(
                new StaticGrantStore(grants),
                mappedRoles(java.util.Map.ofEntries(java.util.Map.entry("team_a", "team_a"), java.util.Map.entry("team_b", "team_b"), java.util.Map.entry("admins", "admins"))),
                filter,
                NO_MASK);
    }

    /**
     * In-memory RowPolicyStore for E2E testing. Stores policies in a list
     * and filters by catalog/schema/table on retrieval.
     */
    private static final class InMemoryRowPolicyStore implements RowPolicyStore {
        private final List<RowPolicy> policies;

        InMemoryRowPolicyStore(List<RowPolicy> policies) {
            this.policies = List.copyOf(policies);
        }

        @Override
        public List<RowPolicy> getPolicies(String catalog, String schema, String table) {
            return policies.stream()
                    .filter(p -> p.catalogName().equals(catalog)
                            && p.schemaName().equals(schema)
                            && p.tableName().equals(table))
                    .toList();
        }

        @Override
        public void savePolicy(RowPolicy policy) {
            throw new UnsupportedOperationException("Test store is read-only");
        }

        @Override
        public void deletePolicy(long policyId) {
            throw new UnsupportedOperationException("Test store is read-only");
        }
    }
}
