package org.pangrova.coordinator.query.security;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.TableModify;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalTableModify;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.sql.SqlKind;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.pangrova.coordinator.query.execution.security.SecurityPolicyApplier;
import org.pangrova.security.rowfilter.RowPolicy;
import org.pangrova.testing.ContainerTestBase;

import java.nio.file.Path;
import java.util.List;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/** Real DELETE/MERGE SQL receives the current PostgreSQL row policies before execution. */
class DmlRlsPolicyInjectionTest extends ContainerTestBase {
    @TempDir Path warehouse;
    private SecuritySqlFixture fixture;

    @BeforeEach
    void initialize() throws Exception { fixture = new SecuritySqlFixture(warehouse); }

    @AfterEach
    void close() throws Exception { if (fixture != null) fixture.close(); }

    @Test
    void deleteOperationPlanReceivesRlsFilter() {
        var applier = install(policy("sales_only", "department = 'sales'", "sales_team"));
        var plan = fixture.compile("DELETE FROM orders");

        var result = applier.applyPolicies(plan, SecuritySqlFixture.session("sales_user", Set.of("sales_team")));

        assertThat(result.plan()).isInstanceOf(LogicalTableModify.class);
        assertThat(((LogicalTableModify) result.plan()).getOperation()).isEqualTo(TableModify.Operation.DELETE);
        var filter = findFilter(result.plan());
        assertThat(filter).as("RLS filter must protect the actual DELETE source").isNotNull();
        assertThat(filter.getCondition()).isInstanceOf(RexCall.class);
        assertThat(((RexCall) filter.getCondition()).getKind()).isEqualTo(SqlKind.EQUALS);
        assertThat(result.planExplanation()).contains("sales");
    }

    @Test
    void mergeOperationSourceScanReceivesRlsFilter() {
        var applier = install(policy("region_restrict", "region IN ('US', 'EU')", "regional_team"));
        fixture.installPolicies(List.of(policy("region_restrict", "region IN ('US', 'EU')", "regional_team"),
                new RowPolicy(0, "source_region_restrict", "default", "default", "source_orders",
                        "region IN ('US', 'EU')", List.of("regional_team"), true, 100, null)));
        var plan = fixture.compile("MERGE INTO orders t USING source_orders s ON t.id = s.id "
                + "WHEN MATCHED THEN UPDATE SET amount = s.amount");

        var result = applier.applyPolicies(plan, SecuritySqlFixture.session("region_user", Set.of("regional_team")));

        assertThat(result.plan()).isInstanceOf(LogicalTableModify.class);
        assertThat(((LogicalTableModify) result.plan()).getOperation()).isEqualTo(TableModify.Operation.MERGE);
        var sourceFilter = findFilter(result.plan());
        assertThat(sourceFilter).as("RLS must protect the actual MERGE source input").isNotNull();
        assertThat(sourceFilter.getInput()).isInstanceOf(org.apache.calcite.rel.core.TableScan.class);
        assertThat(((org.apache.calcite.rel.core.TableScan) sourceFilter.getInput()).getTable().getQualifiedName())
                .containsExactly("default", "default", "source_orders");
        assertThat(result.planExplanation()).contains("US", "EU", "source_orders");
    }

    @Test
    void dmlPlanWithoutMatchingPolicyIsUnchanged() {
        var applier = install(policy("admin_only", "status = 'active'", "admin_team"));
        var plan = fixture.compile("DELETE FROM orders");

        var result = applier.applyPolicies(plan, SecuritySqlFixture.session("other_user", Set.of("other_team")));

        assertThat(findFilter(result.plan())).as("Non-matching policy must not add a predicate").isNull();
    }

    @Test
    void dmlPlanWithCompoundFilterReceivesAndPredicate() {
        var applier = install(policy("compound_restrict", "region = 'US' AND status = 'active'", "us_team"));
        var plan = fixture.compile("DELETE FROM orders");

        var result = applier.applyPolicies(plan, SecuritySqlFixture.session("us_user", Set.of("us_team")));

        var filter = findFilter(result.plan());
        assertThat(filter).isNotNull();
        assertThat(((RexCall) filter.getCondition()).getKind()).isEqualTo(SqlKind.AND);
        assertThat(result.planExplanation()).contains("US", "active");
    }

    @Test
    void securityPolicyApplierAppliesPoliciesOnSelectSubPlanOfDml() {
        var applier = install(policy("amount_gate", "amount > 500", "finance"));
        var plan = fixture.compile("SELECT * FROM orders WHERE id > 10");

        var result = applier.applyPolicies(plan, SecuritySqlFixture.session("finance_user", Set.of("finance")));

        assertThat(findFilterWithKind(result.plan(), SqlKind.GREATER_THAN))
                .as("Numeric row policy must be injected into the DML source").isNotNull();
        assertThat(result.planExplanation()).contains("500", "10");
    }

    @Test
    void effectivePlanDigestChangesWhenRlsDefinitionChanges() {
        var actor = SecuritySqlFixture.session("sales_user", Set.of("sales_team"));
        var base = fixture.compile("SELECT * FROM orders");
        var sales = install(policy("department_gate", "department = 'sales'", "sales_team"))
                .applyPolicies(base, actor);

        var engineering = install(policy("department_gate", "department = 'engineering'", "sales_team"))
                .applyPolicies(base, actor);

        assertThat(sales.effectivePlanDigest()).isNotEqualTo(engineering.effectivePlanDigest());
        assertThat(engineering.planExplanation()).contains("engineering").doesNotContain("'sales'");
    }

    private SecurityPolicyApplier install(RowPolicy policy) {
        fixture.authorizePolicyReaders();
        fixture.installPolicies(List.of(policy));
        return fixture.applier();
    }

    private static RowPolicy policy(String name, String expression, String role) {
        return new RowPolicy(0, name, "default", "default", "orders", expression, List.of(role), true, 100, null);
    }

    private static LogicalFilter findFilter(RelNode node) {
        if (node instanceof LogicalFilter filter) return filter;
        for (RelNode input : node.getInputs()) {
            LogicalFilter found = findFilter(input);
            if (found != null) return found;
        }
        return null;
    }

    private static LogicalFilter findFilterWithKind(RelNode node, SqlKind kind) {
        if (node instanceof LogicalFilter filter && filter.getCondition() instanceof RexCall call
                && call.getKind() == kind) return filter;
        for (RelNode input : node.getInputs()) {
            LogicalFilter found = findFilterWithKind(input, kind);
            if (found != null) return found;
        }
        return null;
    }
}
