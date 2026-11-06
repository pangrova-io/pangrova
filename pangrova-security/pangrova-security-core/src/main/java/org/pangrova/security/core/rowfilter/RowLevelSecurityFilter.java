package org.pangrova.security.core.rowfilter;

import org.pangrova.security.api.identity.UserIdentity;
import org.pangrova.security.core.authorization.DeferredSubjectContext;
import org.pangrova.security.spi.rowfilter.RowFilterSource;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Set;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.pangrova.security.api.rowfilter.ParameterizedFilter;
import org.pangrova.security.api.rowfilter.RowPolicy;
import org.pangrova.persistence.security.api.rowfilter.RowPolicyStore;

/**
 * Evaluates row-level security policies for a given user and table,
 * producing an AND-merged SQL filter expression with user context bound
 * via parameterized values (not string concatenation).
 *
 * <p>User-supplied values (principal, groups, attributes) are never directly
 * interpolated into the SQL expression string. Instead, they are bound as
 * indexed parameters in a {@link ParameterizedFilter} and validated against
 * a strict whitelist pattern at render time, preventing SQL injection.
 */
public class RowLevelSecurityFilter implements RowFilterSource {

    private static final Pattern USER_ATTR_PATTERN = Pattern.compile("user_attribute\\('([^']*)'\\)");

    private final RowPolicyStore store;

    public RowLevelSecurityFilter(RowPolicyStore store) {
        this.store = store;
    }

    /**
     * Returns the RLS filter for the given user and table as a safe SQL expression.
     * User values are validated against a strict whitelist pattern before rendering,
     * preventing SQL injection.
     *
     * @return a safe SQL expression with user values validated and bound, or empty if no policies apply
     * @throws IllegalArgumentException if any user value contains unsafe SQL metacharacters
     */
    public Optional<String> getFilter(UserIdentity user, Set<String> effectiveRoles,
                                      String catalog, String schema, String table) {
        return getParameterizedFilter(user, effectiveRoles, catalog, schema, table)
                .map(ParameterizedFilter::toSafeExpression);
    }

    /**
     * Returns the structured parameterized filter with expression template and bound values
     * separated. Useful for consumers that can handle parameters natively (e.g., Calcite
     * expression trees or PreparedStatement parameters).
     *
     * @return the parameterized filter, or empty if no policies apply
     */
    public Optional<ParameterizedFilter> getParameterizedFilter(UserIdentity user, Set<String> effectiveRoles,
                                                                 String catalog, String schema, String table) {
        List<RowPolicy> applicable = filterApplicable(user, store.getPolicies(catalog, schema, table), effectiveRoles);
        return bindApplicablePolicies(user, applicable);
    }

    /** Renders policies read on the caller's current authorization transaction. */
    public static Optional<String> renderPolicies(
            UserIdentity user, Set<String> effectiveRoles, List<RowPolicy> policies) {
        return bindApplicablePolicies(user, filterApplicable(user, policies, effectiveRoles))
                .map(ParameterizedFilter::toSafeExpression);
    }

    private static Optional<ParameterizedFilter> bindApplicablePolicies(UserIdentity user, List<RowPolicy> applicable) {
        if (applicable.isEmpty()) {
            return Optional.empty();
        }
        List<RowPolicy> sorted = sortPolicies(applicable);
        String merged = mergeExpressions(sorted);
        return Optional.of(bindUserContext(merged, user));
    }

    private static List<RowPolicy> filterApplicable(UserIdentity user, List<RowPolicy> policies,
            Set<String> effectiveRoles) {
        policies.stream().filter(RowPolicy::enabled).forEach(policy ->
                DeferredSubjectContext.requireKnownApplicability(user, effectiveRoles, policy.applyToRoles()));
        return policies.stream()
                .filter(RowPolicy::enabled)
                .filter(p -> p.appliesTo(effectiveRoles))
                .toList();
    }

    private static List<RowPolicy> sortPolicies(List<RowPolicy> policies) {
        return policies.stream()
                .sorted(Comparator.comparingInt(RowPolicy::priority).reversed()
                        .thenComparing(RowPolicy::policyName))
                .toList();
    }

    private static String mergeExpressions(List<RowPolicy> policies) {
        return policies.stream()
                .map(p -> "(" + p.filterExpression() + ")")
                .collect(Collectors.joining(" AND "));
    }

    private static ParameterizedFilter bindUserContext(String expression, UserIdentity user) {
        DeferredSubjectContext.requireStable(user, expression);
        List<ParameterizedFilter.BoundParameter> params = new ArrayList<>();
        int paramIndex = 1;

        // Replace current_user() with parameter placeholder
        if (expression.contains("current_user()")) {
            params.add(new ParameterizedFilter.BoundParameter(
                    "current_user", user.principal(), ParameterizedFilter.ParameterType.STRING));
            expression = expression.replace("current_user()", "$" + paramIndex);
            paramIndex++;
        }

        // Replace current_groups() with parameter placeholder
        if (expression.contains("current_groups()")) {
            String groupsValue = user.groups().stream()
                    .sorted()
                    .collect(Collectors.joining(","));
            params.add(new ParameterizedFilter.BoundParameter(
                    "current_groups", groupsValue, ParameterizedFilter.ParameterType.STRING_LIST));
            expression = expression.replace("current_groups()", "$" + paramIndex);
            paramIndex++;
        }

        // Replace user_attribute('key') with parameter placeholders
        Matcher matcher = USER_ATTR_PATTERN.matcher(expression);
        StringBuilder sb = new StringBuilder();
        while (matcher.find()) {
            String key = matcher.group(1);
            String value = user.attributes().getOrDefault(key, "");
            params.add(new ParameterizedFilter.BoundParameter(
                    "user_attribute_" + key, value, ParameterizedFilter.ParameterType.STRING));
            matcher.appendReplacement(sb, "\\$" + paramIndex);
            paramIndex++;
        }
        matcher.appendTail(sb);
        expression = sb.toString();

        return new ParameterizedFilter(expression, params);
    }
}
