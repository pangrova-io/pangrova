package org.pangrova.coordinator.query.security;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.pangrova.security.api.policy.Privilege;
import org.pangrova.security.api.rowfilter.RowPolicy;
import org.pangrova.security.api.error.AccessDeniedException;
import org.pangrova.testing.testkit.ContainerTestBase;

import java.nio.file.Path;
import java.util.List;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Protected source compilation using real catalog metadata and PostgreSQL authority. */
class DmlWriteSecurityEnforcementTest extends ContainerTestBase {
    @TempDir Path warehouse;
    private SecuritySqlFixture fixture;

    @BeforeEach
    void initialize() throws Exception { fixture = new SecuritySqlFixture(warehouse); }

    @AfterEach
    void close() throws Exception { if (fixture != null) fixture.close(); }

    @Test
    void applyPoliciesRejectsUnauthorizedUser() {
        var plan = fixture.compile("SELECT * FROM orders");
        var actor = SecuritySqlFixture.session("unauthorized_user", Set.of("no_access"));

        assertThatThrownBy(() -> fixture.applier().applyPolicies(plan, actor))
                .isInstanceOf(AccessDeniedException.class);
    }

    @Test
    void applyPoliciesAllowsAuthorizedUser() {
        fixture.grant("admin_role", Privilege.SELECT);
        var plan = fixture.compile("SELECT * FROM orders");
        var actor = SecuritySqlFixture.session("admin", Set.of("admin_role"));

        assertThat(fixture.applier().applyPolicies(plan, actor)).isSameAs(plan);
    }

    @Test
    void applyPoliciesFailsClosedWhenAccessControlEnforcerIsNull() {
        fixture.grant("admin_role", Privilege.SELECT);
        var plan = fixture.compile("SELECT * FROM orders");
        var actor = SecuritySqlFixture.session("admin", Set.of("admin_role"));

        assertThatThrownBy(() -> fixture.withoutAccessEnforcer().applyPolicies(plan, actor))
                .isInstanceOf(AccessDeniedException.class).hasMessageContaining("complete authorization");
    }

    @Test
    void applyPoliciesInjectsRlsFilterIntoPlan() {
        fixture.authorizePolicyReaders();
        fixture.installPolicies(List.of(policy("sales_only", "department = 'sales'", "sales_team")));
        var plan = fixture.compile("SELECT * FROM orders");

        var result = fixture.applier().applyPolicies(plan,
                SecuritySqlFixture.session("sales_user", Set.of("sales_team")));

        assertThat(findFilter(result.plan())).as("RLS filter must be injected into plan").isNotNull();
        assertThat(result.planExplanation()).contains("sales");
    }

    @Test
    void applyPoliciesDoesNotInjectFilterWhenNoPolicyApplies() {
        fixture.authorizePolicyReaders();
        fixture.installPolicies(List.of(policy("admin_only", "status = 'active'", "admin_team")));
        var plan = fixture.compile("SELECT * FROM orders");

        var result = fixture.applier().applyPolicies(plan,
                SecuritySqlFixture.session("other_user", Set.of("other_team")));

        assertThat(findFilter(result.plan())).isNull();
    }

    @Test
    void applyPoliciesEnforcesRlsByRegion() {
        fixture.authorizePolicyReaders();
        fixture.installPolicies(List.of(policy("region_restrict", "region = 'US'", "us_team")));
        var plan = fixture.compile("SELECT * FROM orders");

        var result = fixture.applier().applyPolicies(plan,
                SecuritySqlFixture.session("us_user", Set.of("us_team")));

        assertThat(findFilter(result.plan())).as("RLS filter must restrict visible source rows").isNotNull();
        assertThat(result.planExplanation()).contains("US");
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
}
