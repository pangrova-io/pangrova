package org.pangrova.security.rowfilter;

import org.pangrova.security.api.enums.FilterParameterType;
import org.pangrova.security.api.identity.UserIdentity;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import org.pangrova.security.api.rowfilter.ParameterizedFilter;
import org.pangrova.security.api.rowfilter.RowPolicy;
import org.pangrova.persistence.security.api.rowfilter.RowPolicyStore;

/**
 * Validates: Requirements A.5 — RLS expressions must use parameterized queries
 * instead of SQL string concatenation to prevent SQL injection.
 *
 * <p>Verifies that user-supplied values containing SQL injection payloads are
 * rejected by the parameterized filter validation, rather than being escaped
 * and potentially bypassed.
 */
class RowLevelSecurityInjectionTest {

    private static final String CATALOG = "iceberg";
    private static final String SCHEMA = "default";
    private static final String TABLE = "orders";

    @ParameterizedTest
    @ValueSource(strings = {
            "' OR 1=1 --",
            "'; DROP TABLE orders; --",
            "' UNION SELECT * FROM secrets --",
            "admin'--",
            "x' OR '1'='1",
            "'; EXEC xp_cmdshell('whoami'); --",
            "' AND 1=0 UNION SELECT password FROM users --"
    })
    void rejectsInjectionPayloadInPrincipal(String maliciousPrincipal) {
        RowPolicy policy = new RowPolicy(1L, "owner_filter", CATALOG, SCHEMA, TABLE,
                "owner = current_user()", List.of(), true, 1, null);
        RowLevelSecurityFilter filter = new RowLevelSecurityFilter(singlePolicyStore(policy));
        UserIdentity attacker = new UserIdentity(
                maliciousPrincipal, Set.of("users"), Map.of(), Instant.now());

        assertThatThrownBy(() -> filter.getFilter(attacker, java.util.Set.of(), CATALOG, SCHEMA, TABLE))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("unsafe characters")
                .hasMessageContaining("SQL injection");
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "' OR 1=1 --",
            "engineering'; DROP TABLE users; --",
            "x' UNION SELECT * FROM secrets --"
    })
    void rejectsInjectionPayloadInUserAttribute(String maliciousAttribute) {
        RowPolicy policy = new RowPolicy(1L, "dept_filter", CATALOG, SCHEMA, TABLE,
                "department = user_attribute('department')", List.of(), true, 1, null);
        RowLevelSecurityFilter filter = new RowLevelSecurityFilter(singlePolicyStore(policy));
        UserIdentity attacker = new UserIdentity(
                "alice", Set.of("users"), Map.of("department", maliciousAttribute), Instant.now());

        assertThatThrownBy(() -> filter.getFilter(attacker, java.util.Set.of(), CATALOG, SCHEMA, TABLE))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("unsafe characters")
                .hasMessageContaining("SQL injection");
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "admin'; --",
            "role1','role2') OR ('1'='1"
    })
    void rejectsInjectionPayloadInGroups(String maliciousGroup) {
        RowPolicy policy = new RowPolicy(1L, "group_filter", CATALOG, SCHEMA, TABLE,
                "role IN (current_groups())", List.of(), true, 1, null);
        RowLevelSecurityFilter filter = new RowLevelSecurityFilter(singlePolicyStore(policy));
        UserIdentity attacker = new UserIdentity(
                "alice", Set.of(maliciousGroup), Map.of(), Instant.now());

        assertThatThrownBy(() -> filter.getFilter(attacker, java.util.Set.of(), CATALOG, SCHEMA, TABLE))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("unsafe characters")
                .hasMessageContaining("SQL injection");
    }

    @Test
    void allowsSafePrincipalValues() {
        RowPolicy policy = new RowPolicy(1L, "owner_filter", CATALOG, SCHEMA, TABLE,
                "owner = current_user()", List.of(), true, 1, null);
        RowLevelSecurityFilter filter = new RowLevelSecurityFilter(singlePolicyStore(policy));
        UserIdentity safeUser = new UserIdentity(
                "alice.smith@company.com", Set.of("engineering"), Map.of(), Instant.now());

        assertThat(filter.getFilter(safeUser, java.util.Set.of(), CATALOG, SCHEMA, TABLE))
                .isPresent()
                .hasValueSatisfying(expr -> {
                    assertThat(expr).contains("'alice.smith@company.com'");
                    assertThat(expr).doesNotContain("current_user()");
                });
    }

    @Test
    void allowsSafeAttributeValues() {
        RowPolicy policy = new RowPolicy(1L, "dept_filter", CATALOG, SCHEMA, TABLE,
                "department = user_attribute('department')", List.of(), true, 1, null);
        RowLevelSecurityFilter filter = new RowLevelSecurityFilter(singlePolicyStore(policy));
        UserIdentity safeUser = new UserIdentity(
                "alice", Set.of("users"), Map.of("department", "engineering"), Instant.now());

        assertThat(filter.getFilter(safeUser, java.util.Set.of(), CATALOG, SCHEMA, TABLE))
                .isPresent()
                .hasValueSatisfying(expr -> {
                    assertThat(expr).contains("'engineering'");
                    assertThat(expr).doesNotContain("user_attribute(");
                });
    }

    @Test
    void parameterizedFilterSeparatesTemplateFromValues() {
        RowPolicy policy = new RowPolicy(1L, "owner_filter", CATALOG, SCHEMA, TABLE,
                "owner = current_user()", List.of(), true, 1, null);
        RowLevelSecurityFilter filter = new RowLevelSecurityFilter(singlePolicyStore(policy));
        UserIdentity user = new UserIdentity(
                "alice", Set.of("users"), Map.of(), Instant.now());

        ParameterizedFilter paramFilter = filter.getParameterizedFilter(user, java.util.Set.of(), CATALOG, SCHEMA, TABLE)
                .orElseThrow();

        // Template contains placeholder, not the actual value
        assertThat(paramFilter.expressionTemplate()).contains("$1");
        assertThat(paramFilter.expressionTemplate()).doesNotContain("alice");

        // Parameters carry the value separately
        assertThat(paramFilter.parameters()).hasSize(1);
        assertThat(paramFilter.parameters().get(0).value()).isEqualTo("alice");
        assertThat(paramFilter.parameters().get(0).name()).isEqualTo("current_user");
    }

    @Test
    void parameterizedFilterValidatesOnRender() {
        ParameterizedFilter malicious = new ParameterizedFilter(
                "(owner = $1)",
                List.of(new ParameterizedFilter.BoundParameter(
                        "current_user", "' OR 1=1 --",
                        FilterParameterType.STRING)));

        assertThatThrownBy(malicious::toSafeExpression)
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("unsafe characters");
    }

    private static RowPolicyStore singlePolicyStore(RowPolicy policy) {
        return new RowPolicyStore() {
            private final List<RowPolicy> policies = new ArrayList<>(List.of(policy));

            @Override
            public List<RowPolicy> getPolicies(String catalog, String schema, String table) {
                return List.copyOf(policies);
            }

            @Override
            public void savePolicy(RowPolicy p) {
                policies.add(p);
            }

            @Override
            public void deletePolicy(long policyId) {
                policies.removeIf(p -> p.policyId() == policyId);
            }
        };
    }
}
