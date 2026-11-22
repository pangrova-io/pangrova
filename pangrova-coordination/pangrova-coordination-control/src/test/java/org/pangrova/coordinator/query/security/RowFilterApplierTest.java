package org.pangrova.coordinator.query.security;

import org.pangrova.planner.security.RowFilterApplier;
import org.pangrova.planner.analysis.PangrovaPlanner;
import org.pangrova.planner.analysis.SqlCompiler;
import org.pangrova.security.api.authorization.AccessControl;
import org.pangrova.security.api.exception.AuthenticationRequiredException;
import org.pangrova.security.api.identity.UserIdentity;
import org.pangrova.security.policy.authorization.DynamicRbacAccessControl;
import org.pangrova.testing.testkit.security.policy.StaticGrantStore;
import org.pangrova.security.mask.ColumnMaskingProcessor;
import org.pangrova.testing.testkit.security.mask.EmptyColumnMaskStore;
import org.pangrova.security.rowfilter.RowLevelSecurityFilter;
import org.pangrova.security.api.rowfilter.RowPolicy;
import org.pangrova.persistence.security.api.rowfilter.RowPolicyStore;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

import static org.pangrova.coordinator.testinfra.CoordinatorRoleFixtures.mappedRoles;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests for RowFilterApplier row-level security filter integration.
 * Validates: Requirements 4.1, 4.4.
 */
class RowFilterApplierTest {

    private static final SqlCompiler COMPILER = buildCompilerWithOrdersTable();

    @Test
    void nullIdentityThrowsAuthenticationRequired() {
        RowFilterApplier applier = new RowFilterApplier(buildAccessControlWithPolicies(List.of()));
        RelNode relNode = COMPILER.compileToRelNode("SELECT 1");

        assertThatThrownBy(() -> applier.applyRowFilters(relNode, null))
                .isInstanceOf(AuthenticationRequiredException.class)
                .hasMessageContaining("authenticated identity");
    }

    @Test
    void constantQueryNoTableScanReturnsSameNode() {
        RowFilterApplier applier = new RowFilterApplier(buildAccessControlWithPolicies(List.of()));
        RelNode relNode = COMPILER.compileToRelNode("SELECT 1 + 2");

        RelNode result = applier.applyRowFilters(relNode, testIdentity("user1", Set.of("readers")));

        assertThat(result).isSameAs(relNode);
    }

    @Test
    void emptyPolicyStoreReturnsSameNode() {
        RowFilterApplier applier = new RowFilterApplier(buildAccessControlWithPolicies(List.of()));
        RelNode relNode = COMPILER.compileToRelNode("SELECT 1");

        RelNode result = applier.applyRowFilters(relNode, testIdentity("user1", Set.of("readers")));

        assertThat(result).isSameAs(relNode);
    }

    @Test
    void filterExpressionAppliedToTableScan() {
        RowPolicy policy = new RowPolicy(
                1L, "sales_only", "default", "default", "orders",
                "department = 'sales'", List.of("sales_team"), true, 100, null);
        RowFilterApplier applier = new RowFilterApplier(buildAccessControlWithPolicies(List.of(policy)));
        RelNode relNode = COMPILER.compileToRelNode("SELECT * FROM orders");

        RelNode result = applier.applyRowFilters(relNode, testIdentity("user1", Set.of("sales_team")));

        LogicalFilter filter = findFilter(result);
        assertThat(filter).isNotNull();
        RexNode condition = filter.getCondition();
        assertThat(condition).isInstanceOf(RexCall.class);
        RexCall call = (RexCall) condition;
        assertThat(call.getKind()).isEqualTo(SqlKind.EQUALS);
    }

    @Test
    void selfJoinReadsOnePolicyPerCompilationIncludingAbsence() {
        RowPolicy policy = new RowPolicy(1L, "sales_only", "default", "default", "orders",
                "department = 'sales'", List.of("sales_team"), true, 100, null);
        AtomicInteger reads = new AtomicInteger();
        RowFilterApplier applier = new RowFilterApplier(buildAccessControlWithPolicies(
                () -> reads.getAndIncrement() == 0 ? List.of(policy) : List.of()));
        RelNode plan = COMPILER.compileToRelNode("SELECT a.id FROM orders a JOIN orders b ON a.id = b.id");
        UserIdentity identity = testIdentity("user1", Set.of("sales_team"));

        RelNode filtered = applier.applyRowFilters(plan, identity);
        assertThat(RelOptUtil.toString(filtered).lines().filter(line -> line.contains("LogicalFilter"))).hasSize(2);
        assertThat(reads.get()).isEqualTo(1);

        assertThat(applier.applyRowFilters(plan, identity)).isSameAs(plan);
        assertThat(reads.get()).isEqualTo(2);
    }

    @Test
    void filterRemapsColumnIndicesWhenColumnsReordered() {
        // Table has columns: id(0), department(1), region(2), status(3), amount(4)
        // Query selects only: region(0), department(1) — reordered subset
        // Filter references "department" which is at index 1 in full schema
        // After remapping, it should reference index 1 in the pruned schema (department)
        RowPolicy policy = new RowPolicy(
                1L, "sales_only", "default", "default", "orders",
                "department = 'sales'", List.of("sales_team"), true, 100, null);
        RowFilterApplier applier = new RowFilterApplier(buildAccessControlWithPolicies(List.of(policy)));
        RelNode relNode = COMPILER.compileToRelNode("SELECT region, department FROM orders");

        RelNode result = applier.applyRowFilters(relNode, testIdentity("user1", Set.of("sales_team")));

        LogicalFilter filter = findFilter(result);
        assertThat(filter).isNotNull();
        RexNode condition = filter.getCondition();
        assertThat(condition).isInstanceOf(RexCall.class);
        RexCall call = (RexCall) condition;
        assertThat(call.getKind()).isEqualTo(SqlKind.EQUALS);
    }

    @Test
    void filterRemapsColumnIndicesWhenColumnsPruned() {
        // Table has columns: id(0), department(1), region(2), status(3), amount(4)
        // Filter references "amount" which is at index 4 in full schema
        // Query selects: id, amount — pruned to 2 columns
        // After remapping, "amount" should reference the correct index in the scan's RowType
        RowPolicy policy = new RowPolicy(
                1L, "amount_limit", "default", "default", "orders",
                "amount > 1000", List.of("managers"), true, 100, null);
        RowFilterApplier applier = new RowFilterApplier(buildAccessControlWithPolicies(List.of(policy)));
        RelNode relNode = COMPILER.compileToRelNode("SELECT id, amount FROM orders");

        RelNode result = applier.applyRowFilters(relNode, testIdentity("user1", Set.of("managers")));

        LogicalFilter filter = findFilter(result);
        assertThat(filter).isNotNull();
        RexNode condition = filter.getCondition();
        assertThat(condition).isInstanceOf(RexCall.class);
        RexCall call = (RexCall) condition;
        assertThat(call.getKind()).isEqualTo(SqlKind.GREATER_THAN);
    }

    @Test
    void compoundFilterExpressionApplied() {
        RowPolicy policy = new RowPolicy(
                1L, "region_filter", "default", "default", "orders",
                "region = 'US' AND status = 'active'", List.of("us_team"), true, 100, null);
        RowFilterApplier applier = new RowFilterApplier(buildAccessControlWithPolicies(List.of(policy)));
        RelNode relNode = COMPILER.compileToRelNode("SELECT * FROM orders");

        RelNode result = applier.applyRowFilters(relNode, testIdentity("user1", Set.of("us_team")));

        LogicalFilter filter = findFilter(result);
        assertThat(filter).isNotNull();
        RexNode condition = filter.getCondition();
        assertThat(condition).isInstanceOf(RexCall.class);
        RexCall call = (RexCall) condition;
        assertThat(call.getKind()).isEqualTo(SqlKind.AND);
    }

    @Test
    void inExpressionFilterApplied() {
        RowPolicy policy = new RowPolicy(
                1L, "multi_region", "default", "default", "orders",
                "region IN ('US', 'EU', 'APAC')", List.of("global_team"), true, 100, null);
        RowFilterApplier applier = new RowFilterApplier(buildAccessControlWithPolicies(List.of(policy)));
        RelNode relNode = COMPILER.compileToRelNode("SELECT * FROM orders");

        RelNode result = applier.applyRowFilters(relNode, testIdentity("user1", Set.of("global_team")));

        LogicalFilter filter = findFilter(result);
        assertThat(filter).isNotNull();
        RexNode condition = filter.getCondition();
        assertThat(condition).isInstanceOf(RexCall.class);
    }

    @Test
    void numericComparisonFilterApplied() {
        RowPolicy policy = new RowPolicy(
                1L, "amount_limit", "default", "default", "orders",
                "amount > 1000", List.of("managers"), true, 100, null);
        RowFilterApplier applier = new RowFilterApplier(buildAccessControlWithPolicies(List.of(policy)));
        RelNode relNode = COMPILER.compileToRelNode("SELECT * FROM orders");

        RelNode result = applier.applyRowFilters(relNode, testIdentity("user1", Set.of("managers")));

        LogicalFilter filter = findFilter(result);
        assertThat(filter).isNotNull();
        RexNode condition = filter.getCondition();
        assertThat(condition).isInstanceOf(RexCall.class);
        RexCall call = (RexCall) condition;
        assertThat(call.getKind()).isEqualTo(SqlKind.GREATER_THAN);
    }

    @Test
    void policyNotAppliedWhenUserNotInRole() {
        RowPolicy policy = new RowPolicy(
                1L, "sales_only", "default", "default", "orders",
                "department = 'sales'", List.of("sales_team"), true, 100, null);
        RowFilterApplier applier = new RowFilterApplier(buildAccessControlWithPolicies(List.of(policy)));
        RelNode relNode = COMPILER.compileToRelNode("SELECT * FROM orders");

        RelNode result = applier.applyRowFilters(relNode, testIdentity("user1", Set.of("other_team")));

        assertThat(findFilter(result)).isNull();
    }

    @Test
    void anonymousRequestToRlsProtectedTableThrows401() {
        RowPolicy policy = new RowPolicy(
                1L, "confidential_filter", "default", "default", "orders",
                "classification = 'public'", List.of("all_users"), true, 100, null);
        RowFilterApplier applier = new RowFilterApplier(buildAccessControlWithPolicies(List.of(policy)));
        RelNode relNode = COMPILER.compileToRelNode("SELECT * FROM orders");

        assertThatThrownBy(() -> applier.applyRowFilters(relNode, null))
                .isInstanceOf(AuthenticationRequiredException.class)
                .hasMessageContaining("authenticated identity");
    }

    /**
     * Recursively searches for a LogicalFilter in the RelNode tree.
     * The planner may wrap TableScan with Project, so the filter may not be the root.
     */
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

    /**
     * Builds a SqlCompiler with a schema containing an "orders" table
     * that has columns matching the filter expressions used in tests.
     */
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

    private static AccessControl buildAccessControlWithPolicies(List<RowPolicy> policies) {
        return buildAccessControlWithPolicies(() -> policies);
    }

    private static AccessControl buildAccessControlWithPolicies(Supplier<List<RowPolicy>> policies) {
        RowPolicyStore policyStore = new RowPolicyStore() {
            @Override
            public List<RowPolicy> getPolicies(String c, String s, String t) {
                return policies.get().stream()
                        .filter(p -> p.catalogName().equals(c) && p.schemaName().equals(s)
                                && p.tableName().equals(t))
                        .toList();
            }
            @Override public void savePolicy(RowPolicy p) { }
            @Override public void deletePolicy(long id) { }
        };
        return new DynamicRbacAccessControl(
                new StaticGrantStore(List.of()),
                mappedRoles(java.util.Map.ofEntries(java.util.Map.entry("readers", "readers"), java.util.Map.entry("sales_team", "sales_team"), java.util.Map.entry("managers", "managers"), java.util.Map.entry("us_team", "us_team"), java.util.Map.entry("global_team", "global_team"), java.util.Map.entry("other_team", "other_team"))),
                new RowLevelSecurityFilter(policyStore),
                new ColumnMaskingProcessor(new EmptyColumnMaskStore()));
    }

    private static UserIdentity testIdentity(String principal, Set<String> groups) {
        return new UserIdentity(principal, groups, Map.of(), Instant.now());
    }
}
