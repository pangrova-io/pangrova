package org.pangrova.coordinator.query.resourcegroup;

import java.util.Optional;
import java.util.Set;
import java.util.regex.Pattern;

/**
 * A single resource-group selection rule. A rule {@link #matches} a
 * {@link ResourceGroupSelectionContext} when <em>every</em> constraint it specifies is satisfied;
 * unspecified constraints act as wildcards (match anything). This is the Trino selector model.
 *
 * <p>String criteria (user, source, source IP) are regex-matched (full match). Client tags are
 * matched by subset containment: the rule's required tags must all be present in the context.
 * Query type and database are matched by exact (case-insensitive) equality.</p>
 *
 * @param userPattern    regex the context user must fully match, if constrained
 * @param sourcePattern  regex the context source must fully match, if constrained
 * @param requiredTags   tags that must all be present in the context (empty = unconstrained)
 * @param queryType      exact query type the context must equal (ignoring case), if constrained
 * @param database       exact database the context must equal (ignoring case), if constrained
 * @param sourceIpPattern regex the context source IP must fully match, if constrained
 * @param resourceGroup  the resource-group id assigned when this rule matches
 */
public record ResourceGroupRule(
        Optional<Pattern> userPattern,
        Optional<Pattern> sourcePattern,
        Set<String> requiredTags,
        Optional<String> queryType,
        Optional<String> database,
        Optional<Pattern> sourceIpPattern,
        String resourceGroup) {

    public ResourceGroupRule {
        userPattern = orEmpty(userPattern);
        sourcePattern = orEmpty(sourcePattern);
        requiredTags = requiredTags == null ? Set.of() : Set.copyOf(requiredTags);
        queryType = orEmpty(queryType);
        database = orEmpty(database);
        sourceIpPattern = orEmpty(sourceIpPattern);
        if (resourceGroup == null || resourceGroup.isBlank()) {
            throw new IllegalArgumentException("resourceGroup must not be null or blank");
        }
    }

    private static <T> Optional<T> orEmpty(Optional<T> value) {
        return value == null ? Optional.empty() : value;
    }

    /** @return true when every constraint this rule specifies is satisfied by {@code ctx}. */
    public boolean matches(ResourceGroupSelectionContext ctx) {
        return patternMatches(userPattern, ctx.user())
                && patternMatches(sourcePattern, ctx.source())
                && ctx.clientTags().containsAll(requiredTags)
                && equalsIgnoreCase(queryType, ctx.queryType())
                && equalsIgnoreCase(database, ctx.database())
                && patternMatches(sourceIpPattern, ctx.sourceIp());
    }

    private static boolean patternMatches(Optional<Pattern> pattern, Optional<String> value) {
        if (pattern.isEmpty()) {
            return true;
        }
        return value.isPresent() && pattern.get().matcher(value.get()).matches();
    }

    private static boolean equalsIgnoreCase(Optional<String> expected, Optional<String> value) {
        if (expected.isEmpty()) {
            return true;
        }
        return value.isPresent() && expected.get().equalsIgnoreCase(value.get());
    }

    /** Starts a fluent builder for a rule assigning {@code resourceGroup}. */
    public static Builder toGroup(String resourceGroup) {
        return new Builder(resourceGroup);
    }

    /** Fluent builder; unset criteria remain wildcards. */
    public static final class Builder {
        private final String resourceGroup;
        private Optional<Pattern> userPattern = Optional.empty();
        private Optional<Pattern> sourcePattern = Optional.empty();
        private Set<String> requiredTags = Set.of();
        private Optional<String> queryType = Optional.empty();
        private Optional<String> database = Optional.empty();
        private Optional<Pattern> sourceIpPattern = Optional.empty();

        private Builder(String resourceGroup) {
            this.resourceGroup = resourceGroup;
        }

        public Builder user(String regex) {
            this.userPattern = Optional.of(Pattern.compile(regex));
            return this;
        }

        public Builder source(String regex) {
            this.sourcePattern = Optional.of(Pattern.compile(regex));
            return this;
        }

        public Builder tags(Set<String> tags) {
            this.requiredTags = tags;
            return this;
        }

        public Builder queryType(String type) {
            this.queryType = Optional.of(type);
            return this;
        }

        public Builder database(String db) {
            this.database = Optional.of(db);
            return this;
        }

        public Builder sourceIp(String regex) {
            this.sourceIpPattern = Optional.of(Pattern.compile(regex));
            return this;
        }

        public ResourceGroupRule build() {
            return new ResourceGroupRule(userPattern, sourcePattern, requiredTags,
                    queryType, database, sourceIpPattern, resourceGroup);
        }
    }
}
