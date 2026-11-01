package org.pangrova.coordinator.query.security;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalProject;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.pangrova.coordinator.query.execution.security.SecurityPolicyApplier;
import org.pangrova.spi.security.policy.Privilege;
import org.pangrova.spi.security.mask.ColumnMask;
import org.pangrova.spi.security.rowfilter.RowPolicy;
import org.pangrova.spi.security.AccessDeniedException;
import org.pangrova.testing.testkit.ContainerTestBase;

import java.nio.file.Path;
import java.util.List;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Authorization, row filters and masks share the real catalog and PostgreSQL policy authority. */
class ExplainVsDmlSecurityEnforcementTest extends ContainerTestBase {
    @TempDir Path warehouse;
    private SecuritySqlFixture fixture;

    @BeforeEach
    void initialize() throws Exception { fixture = new SecuritySqlFixture(warehouse); }

    @AfterEach
    void close() throws Exception { if (fixture != null) fixture.close(); }

    @Nested
    class DmlPathFullEnforcement {
        @Test
        void applyPolicies_injectsRlsFilterIntoPlan() {
            var applier = fullApplier(List.of(policy("sales_only", "department = 'sales'", "sales_team")), List.of());
            var plan = fixture.compile("SELECT * FROM orders");

            var result = applier.applyPolicies(plan, SecuritySqlFixture.session("user1", Set.of("sales_team")));

            assertThat(findFilter(result.plan())).as("Source compilation must inject RLS").isNotNull();
            assertThat(result.planExplanation()).contains("sales");
        }

        @Test
        void applyPolicies_injectsColumnMaskIntoPlan() {
            var applier = fullApplier(List.of(), List.of(mask("mask_dept", "analysts")));
            var plan = fixture.compile("SELECT * FROM orders");

            var result = applier.applyPolicies(plan, SecuritySqlFixture.session("user1", Set.of("analysts")));

            assertThat(findMaskedProject(result.plan())).as("Source compilation must inject the actual mask").isNotNull();
            assertThat(result.planExplanation()).contains("***").isNotEqualTo(plan.planExplanation());
        }

        @Test
        void applyPolicies_enforcesAuthorizationOnDml() {
            fixture.grant("role1", Privilege.SELECT);
            var plan = fixture.compile("SELECT * FROM orders");
            var actor = SecuritySqlFixture.session("user1", Set.of("role1"));

            assertThatCode(() -> fixture.applier().applyPolicies(plan, actor)).doesNotThrowAnyException();
        }

        @Test
        void applyPolicies_injectsBothRlsAndColumnMask() {
            var applier = fullApplier(List.of(policy("rls", "amount > 100", "finance")),
                    List.of(mask("mask", "finance")));
            var plan = fixture.compile("SELECT * FROM orders");

            var result = applier.applyPolicies(plan, SecuritySqlFixture.session("user1", Set.of("finance")));

            assertThat(findFilter(result.plan())).isNotNull();
            assertThat(findMaskedProject(result.plan())).isNotNull();
            assertThat(result.planExplanation()).contains("100", "***").isNotEqualTo(plan.planExplanation());
        }
    }

    @Nested
    class AuthorizationEnforcement {
        @Test
        void applyPolicies_allowsAuthorizedUser() {
            fixture.grant("role1", Privilege.SELECT);
            var plan = fixture.compile("SELECT * FROM orders");
            var actor = SecuritySqlFixture.session("user1", Set.of("role1"));

            assertThatCode(() -> fixture.applier().applyPolicies(plan, actor)).doesNotThrowAnyException();
        }

        @Test
        void applyPolicies_rejectsUnauthorizedUser() {
            fixture.grant("role1", Privilege.INSERT);
            var plan = fixture.compile("SELECT * FROM orders");
            var actor = SecuritySqlFixture.session("user1", Set.of("role1"));

            assertThatThrownBy(() -> fixture.applier().applyPolicies(plan, actor))
                    .isInstanceOf(AccessDeniedException.class);
        }

        @Test
        void applyPolicies_rejectsAnonymousAccess() {
            fixture.grant("role1", Privilege.SELECT);
            var plan = fixture.compile("SELECT * FROM orders");
            var actor = SecuritySqlFixture.session(null, Set.of());

            assertThatThrownBy(() -> fixture.applier().applyPolicies(plan, actor))
                    .isInstanceOf(AccessDeniedException.class);
        }
    }

    @Nested
    class NoPolicies {
        @Test
        void noPolicies_planUnchanged() {
            fixture.grant("role1", Privilege.SELECT);
            var plan = fixture.compile("SELECT * FROM orders");

            var result = fixture.applier().applyPolicies(plan, SecuritySqlFixture.session("user1", Set.of("role1")));

            assertThat(result).isSameAs(plan);
        }
    }

    private SecurityPolicyApplier fullApplier(List<RowPolicy> policies, List<ColumnMask> masks) {
        fixture.authorizePolicyReaders();
        fixture.installPolicies(policies);
        fixture.installMasks(masks);
        return fixture.applier();
    }

    private static RowPolicy policy(String name, String expression, String role) {
        return new RowPolicy(0, name, "default", "default", "orders", expression, List.of(role), true, 100, null);
    }

    private static ColumnMask mask(String name, String role) {
        return new ColumnMask(0, name, "default", "default", "orders", "department", "'***'",
                List.of(role), List.of(), true, null);
    }

    private static LogicalFilter findFilter(RelNode node) {
        if (node instanceof LogicalFilter filter) return filter;
        for (RelNode input : node.getInputs()) {
            LogicalFilter found = findFilter(input);
            if (found != null) return found;
        }
        return null;
    }

    private static LogicalProject findMaskedProject(RelNode node) {
        if (node instanceof LogicalProject project
                && project.getProjects().stream().anyMatch(expression -> expression.toString().contains("***"))) {
            return project;
        }
        for (RelNode input : node.getInputs()) {
            LogicalProject found = findMaskedProject(input);
            if (found != null) return found;
        }
        return null;
    }
}
