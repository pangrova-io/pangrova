package org.pangrova.security.rowfilter;

import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.constraints.AlphaChars;
import net.jqwik.api.constraints.StringLength;
import org.pangrova.spi.security.rowfilter.ParameterizedFilter;
import org.pangrova.spi.security.rowfilter.RowPolicy;
import org.pangrova.persistence.api.security.rowfilter.RowPolicyStore;
import org.pangrova.spi.security.UserIdentity;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Property tests for {@link RowLevelSecurityFilter} parameterized SQL injection prevention.
 *
 * <p>Verifies two key properties:
 * <ol>
 *   <li>Safe alphanumeric values always produce valid parameterized output</li>
 *   <li>The expression template never contains raw user values (separation of template and data)</li>
 * </ol>
 */
class RowLevelSecurityFilterPropertyTest {

    @Property(tries = 50)
    void safeAlphanumericPrincipal_alwaysProducesValidFilter(
            @ForAll @AlphaChars @StringLength(min = 1, max = 50) String principal) {

        RowPolicy policy = new RowPolicy(
                1L, "test", "cat", "sch", "tbl",
                "owner = current_user()",
                List.of(), true, 1, null);

        RowPolicyStore store = singlePolicyStore(policy);
        RowLevelSecurityFilter filter = new RowLevelSecurityFilter(store);

        UserIdentity user = new UserIdentity(principal, Set.of("role1"), Map.of(), Instant.now());
        Optional<String> result = filter.getFilter(user, java.util.Set.of("role1"), "cat", "sch", "tbl");

        assertThat(result).isPresent();
        String sql = result.get();
        // Safe values produce balanced quotes
        long quoteCount = sql.chars().filter(c -> c == '\'').count();
        assertThat(quoteCount % 2)
                .as("SQL expression must have balanced single quotes: %s", sql)
                .isEqualTo(0);
        // The rendered expression contains the principal as a quoted literal
        assertThat(sql).contains("'" + principal + "'");
    }

    @Property(tries = 50)
    void safeAlphanumericAttribute_alwaysProducesValidFilter(
            @ForAll @AlphaChars @StringLength(min = 1, max = 50) String attrValue) {

        RowPolicy policy = new RowPolicy(
                1L, "test", "cat", "sch", "tbl",
                "dept = user_attribute('department')",
                List.of(), true, 1, null);

        RowPolicyStore store = singlePolicyStore(policy);
        RowLevelSecurityFilter filter = new RowLevelSecurityFilter(store);

        UserIdentity user = new UserIdentity("alice", Set.of("role1"),
                Map.of("department", attrValue), Instant.now());
        Optional<String> result = filter.getFilter(user, java.util.Set.of("role1"), "cat", "sch", "tbl");

        assertThat(result).isPresent();
        long quoteCount = result.get().chars().filter(c -> c == '\'').count();
        assertThat(quoteCount % 2)
                .as("SQL expression must have balanced single quotes: %s", result.get())
                .isEqualTo(0);
    }

    @Property(tries = 50)
    void parameterizedTemplate_neverContainsRawUserValue(
            @ForAll @AlphaChars @StringLength(min = 3, max = 50) String principal) {

        RowPolicy policy = new RowPolicy(
                1L, "test", "cat", "sch", "tbl",
                "owner = current_user()",
                List.of(), true, 1, null);

        RowPolicyStore store = singlePolicyStore(policy);
        RowLevelSecurityFilter filter = new RowLevelSecurityFilter(store);

        UserIdentity user = new UserIdentity(principal, Set.of("role1"), Map.of(), Instant.now());
        Optional<ParameterizedFilter> paramFilter =
                filter.getParameterizedFilter(user, java.util.Set.of("role1"), "cat", "sch", "tbl");

        assertThat(paramFilter).isPresent();
        // The template must NOT contain the raw user value — only placeholders
        assertThat(paramFilter.get().expressionTemplate()).doesNotContain(principal);
        // The parameter list must contain the value
        assertThat(paramFilter.get().parameters())
                .anyMatch(p -> p.value().equals(principal));
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
