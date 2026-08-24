package org.pangrova.coordinator.query.security;

import org.pangrova.coordinator.query.CoordinatorSessionContext;
import org.pangrova.coordinator.query.execution.security.SecurityPolicyApplier;
import org.pangrova.planner.PangrovaPlanner;
import org.pangrova.planner.SqlCompiler;
import org.pangrova.planner.security.ColumnMaskApplier;
import org.pangrova.planner.security.RowFilterApplier;
import org.pangrova.planner.security.SecurityExpressionCache;
import org.pangrova.security.authz.Privilege;
import org.pangrova.security.authz.PrivilegeGrant;
import org.pangrova.security.authz.RbacAccessControl;
import org.pangrova.security.mask.ColumnMask;
import org.pangrova.security.mask.ColumnMaskingProcessor;
import org.pangrova.security.mask.EmptyColumnMaskStore;
import org.pangrova.security.mask.EnrichedColumnMaskStore;
import org.pangrova.security.rowfilter.EmptyRowPolicyStore;
import org.pangrova.security.rowfilter.RowLevelSecurityFilter;
import org.pangrova.security.rowfilter.RowPolicy;
import org.pangrova.security.rowfilter.RowPolicyStore;
import org.pangrova.spi.security.AccessControl;
import org.pangrova.spi.security.AccessDeniedException;
import org.pangrova.spi.security.UserIdentity;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * F-CHAIN-SEC-09: Verifies that the DML path enforces full security policies
 * (authorization + RLS + column masking) via {@link SecurityPolicyApplier#applyPolicies}.
 *
 * <p>Tests that applyPolicies injects RLS filters and column masks into the plan
 * when active policies exist, and that authorization is enforced.</p>
 */
class ExplainVsDmlSecurityEnforcementTest {

    private static final SqlCompiler COMPILER = buildCompilerWithOrdersTable();

    // -- DML path: applyPolicies (full enforcement) --

    @Nested
    class DmlPathFullEnforcement {

        @Test
        void applyPolicies_injectsRlsFilterIntoPlan() {
            // Given: an active RLS policy
            RowPolicy policy = new RowPolicy(
                    1L, "sales_only", "default", "default", "orders",
                    "department = 'sales'", List.of("sales_team"), true, 100);
            SecurityPolicyApplier applier = buildFullApplier(List.of(policy), List.of());
            CoordinatorSessionContext session = testSession("user1", Set.of("sales_team"));

            // When: applyPolicies is called (DML path)
            org.pangrova.planner.PlannedQuery plan = COMPILER.compileToPlannedQuery("SELECT * FROM orders");
            org.pangrova.planner.PlannedQuery result = applier.applyPolicies(plan, session);

            // Then: RLS filter IS injected
            assertThat(findFilter(result.plan()))
                    .as("DML path must inject RLS filters via applyPolicies")
                    .isNotNull();
        }

        @Test
        void applyPolicies_injectsColumnMaskIntoPlan() {
            // Given: an active column mask
            ColumnMask mask = new ColumnMask(
                    1L, "mask_dept", "default", "default", "orders",
                    "department", "'***'", List.of("analysts"), List.of(), true);
            SecurityPolicyApplier applier = buildFullApplier(List.of(), List.of(mask));
            CoordinatorSessionContext session = testSession("user1", Set.of("analysts"));

            // When: applyPolicies is called (DML path)
            org.pangrova.planner.PlannedQuery plan = COMPILER.compileToPlannedQuery("SELECT * FROM orders");
            org.pangrova.planner.PlannedQuery result = applier.applyPolicies(plan, session);

            // Then: column mask projection IS injected
            assertThat(findProject(result.plan()))
                    .as("DML path must inject column masks via applyPolicies")
                    .isNotNull();
        }

        @Test
        void applyPolicies_enforcesAuthorizationOnDml() {
            // Given: user has SELECT privilege
            SecurityPolicyApplier applier = buildApplierWithGrants(
                    List.of(grant(Privilege.SELECT)));
            CoordinatorSessionContext session = testSession("user1", Set.of("role1"));

            // When/Then: authorization check passes for SELECT plan
            org.pangrova.planner.PlannedQuery plan = COMPILER.compileToPlannedQuery("SELECT * FROM orders");
            assertThatCode(() -> applier.applyPolicies(plan, session))
                    .doesNotThrowAnyException();
        }

        @Test
        void applyPolicies_injectsBothRlsAndColumnMask() {
            // Given: both RLS and column mask policies active
            RowPolicy rlsPolicy = new RowPolicy(
                    1L, "rls", "default", "default", "orders",
                    "amount > 100", List.of("finance"), true, 100);
            ColumnMask mask = new ColumnMask(
                    1L, "mask", "default", "default", "orders",
                    "department", "'***'", List.of("finance"), List.of(), true);
            SecurityPolicyApplier applier = buildFullApplier(
                    List.of(rlsPolicy), List.of(mask));
            CoordinatorSessionContext session = testSession("user1", Set.of("finance"));

            // When: applyPolicies is called (DML path)
            org.pangrova.planner.PlannedQuery plan = COMPILER.compileToPlannedQuery("SELECT * FROM orders");
            org.pangrova.planner.PlannedQuery result = applier.applyPolicies(plan, session);

            // Then: both RLS filter and column mask are injected
            assertThat(result.planExplanation())
                    .as("DML plan must show both RLS and mask transformations")
                    .isNotEqualTo(plan.planExplanation());
        }
    }

    // -- Authorization enforcement tests --

    @Nested
    class AuthorizationEnforcement {

        @Test
        void applyPolicies_allowsAuthorizedUser() {
            SecurityPolicyApplier applier = buildApplierWithGrants(
                    List.of(grant(Privilege.SELECT)));
            CoordinatorSessionContext session = testSession("user1", Set.of("role1"));

            org.pangrova.planner.PlannedQuery plan = COMPILER.compileToPlannedQuery("SELECT * FROM orders");
            assertThatCode(() -> applier.applyPolicies(plan, session))
                    .doesNotThrowAnyException();
        }

        @Test
        void applyPolicies_rejectsUnauthorizedUser() {
            SecurityPolicyApplier applier = buildApplierWithGrants(
                    List.of(grant(Privilege.INSERT)));
            CoordinatorSessionContext session = testSession("user1", Set.of("role1"));

            org.pangrova.planner.PlannedQuery plan = COMPILER.compileToPlannedQuery("SELECT * FROM orders");
            assertThatThrownBy(() -> applier.applyPolicies(plan, session))
                    .isInstanceOfAny(AccessDeniedException.class,
                            org.pangrova.security.authz.AuthorizationDeniedException.class);
        }

        @Test
        void applyPolicies_rejectsAnonymousAccess() {
            SecurityPolicyApplier applier = buildApplierWithGrants(
                    List.of(grant(Privilege.SELECT)));
            CoordinatorSessionContext session = testSession(null);

            org.pangrova.planner.PlannedQuery plan = COMPILER.compileToPlannedQuery("SELECT * FROM orders");
            assertThatThrownBy(() -> applier.applyPolicies(plan, session))
                    .isInstanceOf(AccessDeniedException.class);
        }
    }

    // -- No-policy tests --

    @Nested
    class NoPolicies {

        @Test
        void noPolicies_planUnchanged() {
            // No RLS, no masks, no access control
            SecurityPolicyApplier applier = new SecurityPolicyApplier(
                    COMPILER, null, null, null);
            CoordinatorSessionContext session = testSession("user1", Set.of());

            org.pangrova.planner.PlannedQuery plan = COMPILER.compileToPlannedQuery("SELECT * FROM orders");
            org.pangrova.planner.PlannedQuery result = applier.applyPolicies(plan, session);

            assertThat(result).isSameAs(plan);
        }
    }

    // -- Helpers --

    private static SecurityPolicyApplier buildFullApplier(
            List<RowPolicy> rlsPolicies, List<ColumnMask> masks) {
        AccessControl ac = buildAccessControl(rlsPolicies, masks);
        return new SecurityPolicyApplier(
                COMPILER,
                null,
                masks.isEmpty() ? null : new ColumnMaskApplier(ac),
                rlsPolicies.isEmpty() ? null : new RowFilterApplier(ac, new SecurityExpressionCache()));
    }

    private static SecurityPolicyApplier buildApplierWithGrants(List<PrivilegeGrant> grants) {
        AccessControl ac = new RbacAccessControl(
                grants,
                new RowLevelSecurityFilter(new EmptyRowPolicyStore()),
                new ColumnMaskingProcessor(new EmptyColumnMaskStore()));
        return new SecurityPolicyApplier(
                COMPILER,
                new AccessControlEnforcer(ac),
                null,
                null);
    }

    private static AccessControl buildAccessControl(
            List<RowPolicy> rlsPolicies, List<ColumnMask> masks) {
        RowPolicyStore policyStore = new RowPolicyStore() {
            @Override
            public List<RowPolicy> getPolicies(String c, String s, String t) {
                return rlsPolicies.stream()
                        .filter(p -> p.catalogName().equals(c)
                                && p.schemaName().equals(s)
                                && p.tableName().equals(t))
                        .toList();
            }
            @Override public void savePolicy(RowPolicy p) { }
            @Override public void deletePolicy(long id) { }
        };
        ColumnMaskingProcessor maskProcessor;
        if (masks.isEmpty()) {
            maskProcessor = new ColumnMaskingProcessor(new EmptyColumnMaskStore());
        } else {
            EnrichedColumnMaskStore maskStore = new EnrichedColumnMaskStore() {
                @Override
                public List<ColumnMask> getColumnMasks(String c, String s, String t, String col) {
                    return masks.stream()
                            .filter(m -> m.catalogName().equals(c) && m.schemaName().equals(s)
                                    && m.tableName().equals(t) && m.columnName().equals(col))
                            .toList();
                }
                @Override public void saveColumnMask(ColumnMask m) { }
                @Override public void deleteMask(long id) { }
                @Override
                public List<org.pangrova.spi.security.ColumnMaskRecord> getMasks(
                        String c, String s, String t, String col) {
                    return getColumnMasks(c, s, t, col).stream()
                            .map(ColumnMask::toRecord).toList();
                }
                @Override public void saveMask(org.pangrova.spi.security.ColumnMaskRecord m) { }
            };
            maskProcessor = new ColumnMaskingProcessor(maskStore);
        }
        return new RbacAccessControl(
                List.of(),
                new RowLevelSecurityFilter(policyStore),
                maskProcessor);
    }

    private static PrivilegeGrant grant(Privilege privilege) {
        return PrivilegeGrant.of("role1", privilege, "TABLE",
                "default", "default", "orders", null);
    }

    private static LogicalFilter findFilter(RelNode node) {
        if (node instanceof LogicalFilter filter) {
            return filter;
        }
        for (RelNode input : node.getInputs()) {
            LogicalFilter found = findFilter(input);
            if (found != null) {
                return found;
            }
        }
        return null;
    }

    private static LogicalProject findProject(RelNode node) {
        if (node instanceof LogicalProject project) {
            return project;
        }
        for (RelNode input : node.getInputs()) {
            LogicalProject found = findProject(input);
            if (found != null) {
                return found;
            }
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
                "test-query-sec09",
                Map.of(),
                new UserIdentity(principal, groups, Map.of(), Instant.now()),
                null);
    }

    private static CoordinatorSessionContext testSession(String principal) {
        if (principal == null) {
            return new CoordinatorSessionContext(
                    "test-query-sec09-anon",
                    Map.of(),
                    null,
                    null);
        }
        return testSession(principal, Set.of());
    }
}
