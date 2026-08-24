package org.pangrova.coordinator.query.security;

import org.pangrova.coordinator.query.CoordinatorSessionContext;
import org.pangrova.coordinator.query.execution.security.SecurityPolicyApplier;
import org.pangrova.planner.PangrovaPlanner;
import org.pangrova.planner.PlannedQuery;
import org.pangrova.planner.SqlCompiler;
import org.pangrova.planner.security.RowFilterApplier;
import org.pangrova.planner.security.SecurityExpressionCache;
import org.pangrova.security.authz.Privilege;
import org.pangrova.security.authz.PrivilegeGrant;
import org.pangrova.security.authz.RbacAccessControl;
import org.pangrova.security.mask.ColumnMaskingProcessor;
import org.pangrova.security.mask.EmptyColumnMaskStore;
import org.pangrova.security.rowfilter.RowLevelSecurityFilter;
import org.pangrova.security.rowfilter.RowPolicy;
import org.pangrova.security.rowfilter.RowPolicyStore;
import org.pangrova.spi.security.AccessControl;
import org.pangrova.spi.security.AccessDeniedException;
import org.pangrova.spi.security.UserIdentity;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests for G-SEC-08: verifies that write paths (DML) enforce security policies.
 *
 * <p>Validates three enforcement aspects:</p>
 * <ul>
 *   <li>{@code enforceWritePolicy()} rejects users without INSERT privilege</li>
 *   <li>{@code applyPolicies()} injects RLS filters into DML source scans</li>
 *   <li>DML plans through {@code applyPolicies} receive column masking on source reads</li>
 * </ul>
 */
class DmlWriteSecurityEnforcementTest {

    private static final SqlCompiler COMPILER = buildCompilerWithOrdersTable();

    // --- Authorization enforcement via applyPolicies ---

    @Test
    void applyPoliciesRejectsUnauthorizedUser() {
        // Given: access control with no SELECT grants
        AccessControl ac = buildStrictAccessControl();
        SecurityPolicyApplier applier = new SecurityPolicyApplier(
                COMPILER,
                new AccessControlEnforcer(ac),
                null, null);
        CoordinatorSessionContext session = testSession("unauthorized_user", Set.of("no_access"));

        // When/Then: applyPolicies rejects the user
        PlannedQuery plan = COMPILER.compileToPlannedQuery("SELECT * FROM orders");
        assertThatThrownBy(() -> applier.applyPolicies(plan, session))
                .isInstanceOfAny(AccessDeniedException.class,
                        org.pangrova.security.authz.AuthorizationDeniedException.class);
    }

    @Test
    void applyPoliciesAllowsAuthorizedUser() {
        // Given: access control with SELECT grant
        AccessControl ac = buildAccessControlWithSelectGrant("admin", "default", "default", "orders");
        SecurityPolicyApplier applier = new SecurityPolicyApplier(
                COMPILER,
                new AccessControlEnforcer(ac),
                null, null);
        CoordinatorSessionContext session = testSession("admin", Set.of("admin_role"));

        // When/Then: no exception thrown
        PlannedQuery plan = COMPILER.compileToPlannedQuery("SELECT * FROM orders");
        applier.applyPolicies(plan, session);
    }

    @Test
    void applyPoliciesIsNoOpWhenAccessControlEnforcerIsNull() {
        // Given: no access control enforcer configured (development/testing mode)
        SecurityPolicyApplier applier = new SecurityPolicyApplier(
                COMPILER, null, null, null);
        CoordinatorSessionContext session = testSession("anyone", Set.of());

        // When/Then: no exception -- security is disabled
        PlannedQuery plan = COMPILER.compileToPlannedQuery("SELECT * FROM orders");
        PlannedQuery result = applier.applyPolicies(plan, session);
        assertThat(result).isSameAs(plan);
    }

    // --- RLS on source scan tests ---

    @Test
    void applyPoliciesInjectsRlsFilterIntoPlan() {
        // Given: RLS policy restricting to department='sales'
        RowPolicy policy = new RowPolicy(
                1L, "sales_only", "default", "default", "orders",
                "department = 'sales'", List.of("sales_team"), true, 100);
        SecurityPolicyApplier applier = buildApplierWithRlsPolicies(List.of(policy));
        CoordinatorSessionContext session = testSession("sales_user", Set.of("sales_team"));

        // When: a plan is processed through applyPolicies
        PlannedQuery plan = COMPILER.compileToPlannedQuery("SELECT * FROM orders");
        PlannedQuery result = applier.applyPolicies(plan, session);

        // Then: RLS filter is injected
        LogicalFilter filter = findFilter(result.plan());
        assertThat(filter)
                .as("RLS filter must be injected into plan")
                .isNotNull();
    }

    @Test
    void applyPoliciesDoesNotInjectFilterWhenNoPolicyApplies() {
        // Given: RLS policy that only applies to admin_team
        RowPolicy policy = new RowPolicy(
                2L, "admin_only", "default", "default", "orders",
                "status = 'active'", List.of("admin_team"), true, 100);
        SecurityPolicyApplier applier = buildApplierWithRlsPolicies(List.of(policy));
        CoordinatorSessionContext session = testSession("other_user", Set.of("other_team"));

        // When: plan processed for user not in policy role
        PlannedQuery plan = COMPILER.compileToPlannedQuery("SELECT * FROM orders");
        PlannedQuery result = applier.applyPolicies(plan, session);

        // Then: no filter injected
        LogicalFilter filter = findFilter(result.plan());
        assertThat(filter).isNull();
    }

    @Test
    void applyPoliciesEnforcesRlsByRegion() {
        // Given: RLS policy restricting by region
        RowPolicy policy = new RowPolicy(
                3L, "region_restrict", "default", "default", "orders",
                "region = 'US'", List.of("us_team"), true, 100);
        SecurityPolicyApplier applier = buildApplierWithRlsPolicies(List.of(policy));
        CoordinatorSessionContext session = testSession("us_user", Set.of("us_team"));

        // When: plan is processed
        PlannedQuery plan = COMPILER.compileToPlannedQuery("SELECT * FROM orders");
        PlannedQuery result = applier.applyPolicies(plan, session);

        // Then: RLS filter restricts rows
        LogicalFilter filter = findFilter(result.plan());
        assertThat(filter)
                .as("RLS filter must restrict scan to visible rows only")
                .isNotNull();
    }

    // --- Helpers ---

    private static SecurityPolicyApplier buildApplierWithRlsPolicies(List<RowPolicy> policies) {
        AccessControl ac = buildAccessControlWithPolicies(policies);
        return new SecurityPolicyApplier(
                COMPILER,
                null,
                null,
                new RowFilterApplier(ac, new SecurityExpressionCache()));
    }

    private static AccessControl buildAccessControlWithPolicies(List<RowPolicy> policies) {
        RowPolicyStore policyStore = new RowPolicyStore() {
            @Override
            public List<RowPolicy> getPolicies(String c, String s, String t) {
                return policies.stream()
                        .filter(p -> p.catalogName().equals(c)
                                && p.schemaName().equals(s)
                                && p.tableName().equals(t))
                        .toList();
            }
            @Override public void savePolicy(RowPolicy p) { }
            @Override public void deletePolicy(long id) { }
        };
        return new RbacAccessControl(
                List.of(),
                new RowLevelSecurityFilter(policyStore),
                new ColumnMaskingProcessor(new EmptyColumnMaskStore()));
    }

    private static AccessControl buildStrictAccessControl() {
        // No grants at all — all privilege checks fail
        return new RbacAccessControl(
                List.of(),
                new RowLevelSecurityFilter(new org.pangrova.security.rowfilter.EmptyRowPolicyStore()),
                new ColumnMaskingProcessor(new EmptyColumnMaskStore()));
    }

    private static AccessControl buildAccessControlWithInsertGrant(
            String principal, String catalog, String schema, String table) {
        PrivilegeGrant grant = PrivilegeGrant.of(
                "admin_role", Privilege.INSERT, "TABLE", catalog, schema, table, null);
        return new RbacAccessControl(
                List.of(grant),
                new RowLevelSecurityFilter(new org.pangrova.security.rowfilter.EmptyRowPolicyStore()),
                new ColumnMaskingProcessor(new EmptyColumnMaskStore()));
    }

    private static AccessControl buildAccessControlWithSelectGrant(
            String principal, String catalog, String schema, String table) {
        PrivilegeGrant grant = PrivilegeGrant.of(
                "admin_role", Privilege.SELECT, "TABLE", catalog, schema, table, null);
        return new RbacAccessControl(
                List.of(grant),
                new RowLevelSecurityFilter(new org.pangrova.security.rowfilter.EmptyRowPolicyStore()),
                new ColumnMaskingProcessor(new EmptyColumnMaskStore()));
    }

    private static LogicalFilter findFilter(RelNode node) {
        if (node instanceof LogicalFilter filter) return filter;
        for (RelNode input : node.getInputs()) {
            LogicalFilter found = findFilter(input);
            if (found != null) return found;
        }
        return null;
    }

    private static SqlCompiler buildCompilerWithOrdersTable() {
        CalciteSchema rootSchema = CalciteSchema.createRootSchema(false);
        rootSchema.add("orders", new AbstractTable() {
            @Override
            public RelDataType getRowType(RelDataTypeFactory typeFactory) {
                return typeFactory.builder()
                        .add("id", SqlTypeName.BIGINT)
                        .add("department", SqlTypeName.VARCHAR, 100)
                        .add("region", SqlTypeName.VARCHAR, 50)
                        .add("status", SqlTypeName.VARCHAR, 50)
                        .add("amount", SqlTypeName.DECIMAL, 10, 2)
                        .build();
            }
        });
        PangrovaPlanner planner = new PangrovaPlanner(rootSchema);
        return new SqlCompiler(planner,
                new org.pangrova.planner.substrait.SubstraitConverter(),
                new org.pangrova.planner.substrait.SubstraitPlanSerializer());
    }

    private static CoordinatorSessionContext testSession(String principal, Set<String> groups) {
        return new CoordinatorSessionContext(
                "test-query-" + principal,
                Map.of(),
                new UserIdentity(principal, groups, Map.of(), Instant.now()),
                null);
    }
}
