package org.pangrova.security.rowfilter;

import org.pangrova.spi.security.UserIdentity;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.pangrova.security.authz.AuthorizationDeniedException;
import org.pangrova.security.authz.DeferredSubjectContext;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Validates: Requirements 18.7 — RowLevelSecurityFilter merges policies and binds user context.
 */
class RowLevelSecurityFilterTest {

    private static final UserIdentity ANALYST = new UserIdentity(
            "alice", Set.of("analyst"), Map.of("department", "engineering"), Instant.now());
    private static final UserIdentity ADMIN = new UserIdentity(
            "bob", Set.of("admin"), Map.of(), Instant.now());

    @ParameterizedTest
    @ValueSource(strings = {"current_groups()", "CURRENT_GROUPS ( )",
            "user_attribute('department')", "USER_ATTRIBUTE ( 'department' )"})
    void deferredPrincipalCannotBindMissingAuthenticationContextAsEmpty(String contextFunction) {
        var subject = DeferredSubjectContext.fromPrincipal("alice", "tenant-one");
        var policy = new RowPolicy(90, "deferred_context", "lake", "default", "orders",
                "department = " + contextFunction, List.of("reader"), true, 1, null);
        var filter = new RowLevelSecurityFilter(new InMemoryRowPolicyStore(List.of(policy)));

        assertThatThrownBy(() -> filter.getFilter(subject, Set.of("reader"), "lake", "default", "orders"))
                .isInstanceOf(AuthorizationDeniedException.class).hasMessageContaining("identity-context");
    }

    @Test
    void deferredPrincipalStillUsesCurrentPersistedRolesAndStablePrincipal() {
        var subject = DeferredSubjectContext.fromPrincipal("alice", "tenant-one");
        var policy = new RowPolicy(90, "owner", "lake", "default", "orders",
                "owner = current_user()", List.of("reader"), true, 1, null);
        var filter = new RowLevelSecurityFilter(new InMemoryRowPolicyStore(List.of(policy)));

        assertThat(subject.groups()).isEmpty();
        assertThat(subject.attributes()).containsEntry("tenant_id", "tenant-one");
        assertThat(filter.getFilter(subject, Set.of("reader"), "lake", "default", "orders"))
                .hasValue("(owner = 'alice')");
        assertThat(filter.getFilter(subject, Set.of(), "lake", "default", "orders")).isEmpty();
    }

    @Test
    void returnsEmptyWhenNoPolicies() {
        RowLevelSecurityFilter filter = new RowLevelSecurityFilter(new EmptyRowPolicyStore());
        Optional<String> result = filter.getFilter(ANALYST, java.util.Set.of("analyst"), "iceberg", "default", "orders");
        assertThat(result).isEmpty();
    }

    @Test
    void returnsSinglePolicyExpression() {
        RowPolicy policy = new RowPolicy(1, "dept_filter", "iceberg", "default", "orders",
                "department = current_user()", List.of(), true, 1, null);
        InMemoryRowPolicyStore store = new InMemoryRowPolicyStore(List.of(policy));
        RowLevelSecurityFilter filter = new RowLevelSecurityFilter(store);

        Optional<String> result = filter.getFilter(ANALYST, java.util.Set.of("analyst"), "iceberg", "default", "orders");
        assertThat(result).isPresent();
        assertThat(result.get()).contains("'alice'");
    }

    @Test
    void mergesMultiplePoliciesWithAnd() {
        RowPolicy p1 = new RowPolicy(1, "a_filter", "iceberg", "default", "orders",
                "region = 'US'", List.of(), true, 1, null);
        RowPolicy p2 = new RowPolicy(2, "b_filter", "iceberg", "default", "orders",
                "status = 'active'", List.of(), true, 2, null);
        InMemoryRowPolicyStore store = new InMemoryRowPolicyStore(List.of(p1, p2));
        RowLevelSecurityFilter filter = new RowLevelSecurityFilter(store);

        Optional<String> result = filter.getFilter(ANALYST, java.util.Set.of("analyst"), "iceberg", "default", "orders");
        assertThat(result).isPresent();
        assertThat(result.get()).contains("AND");
    }

    @Test
    void skipsDisabledPolicies() {
        RowPolicy disabled = new RowPolicy(1, "disabled", "iceberg", "default", "orders",
                "1=0", List.of(), false, 1, null);
        InMemoryRowPolicyStore store = new InMemoryRowPolicyStore(List.of(disabled));
        RowLevelSecurityFilter filter = new RowLevelSecurityFilter(store);

        Optional<String> result = filter.getFilter(ANALYST, java.util.Set.of("analyst"), "iceberg", "default", "orders");
        assertThat(result).isEmpty();
    }

    @Test
    void appliesOnlyToMatchingRoles() {
        RowPolicy policy = new RowPolicy(1, "analyst_only", "iceberg", "default", "orders",
                "visible = true", List.of("analyst"), true, 1, null);
        InMemoryRowPolicyStore store = new InMemoryRowPolicyStore(List.of(policy));
        RowLevelSecurityFilter filter = new RowLevelSecurityFilter(store);

        assertThat(filter.getFilter(ANALYST, java.util.Set.of("analyst"), "iceberg", "default", "orders")).isPresent();
        assertThat(filter.getFilter(ADMIN, java.util.Set.of("admin"), "iceberg", "default", "orders")).isEmpty();
    }

    @Test
    void bindsUserAttributeFunction() {
        RowPolicy policy = new RowPolicy(1, "attr_filter", "iceberg", "default", "orders",
                "dept = user_attribute('department')", List.of(), true, 1, null);
        InMemoryRowPolicyStore store = new InMemoryRowPolicyStore(List.of(policy));
        RowLevelSecurityFilter filter = new RowLevelSecurityFilter(store);

        Optional<String> result = filter.getFilter(ANALYST, java.util.Set.of("analyst"), "iceberg", "default", "orders");
        assertThat(result).isPresent();
        assertThat(result.get()).contains("'engineering'");
    }

    @Test
    void bindsCurrentGroupsFunction() {
        RowPolicy policy = new RowPolicy(1, "group_filter", "iceberg", "default", "orders",
                "role IN (current_groups())", List.of(), true, 1, null);
        InMemoryRowPolicyStore store = new InMemoryRowPolicyStore(List.of(policy));
        RowLevelSecurityFilter filter = new RowLevelSecurityFilter(store);

        Optional<String> result = filter.getFilter(ANALYST, java.util.Set.of("analyst"), "iceberg", "default", "orders");
        assertThat(result).isPresent();
        assertThat(result.get()).contains("'analyst'");
    }

    /**
     * Simple in-memory store for testing.
     */
    private static class InMemoryRowPolicyStore implements RowPolicyStore {
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
            throw new UnsupportedOperationException();
        }

        @Override
        public void deletePolicy(long policyId) {
            throw new UnsupportedOperationException();
        }
    }
}
