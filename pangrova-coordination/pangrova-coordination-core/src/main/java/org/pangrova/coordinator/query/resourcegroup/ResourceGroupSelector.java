package org.pangrova.coordinator.query.resourcegroup;

import java.util.List;
import java.util.Objects;

/**
 * Maps inbound queries to a resource group via ordered, first-match-wins rules.
 *
 * <p>This is the classification layer Pangrova previously lacked: scheduling had only load-based
 * placement plus an inert fair-share factor, with no way to route queries by
 * user/source/client-tags/query-type/database/source-ip into resource groups. The resolved group
 * id is intended to drive both admission and worker placement.</p>
 *
 * <p>Rules are evaluated in list order; the first rule whose {@link ResourceGroupRule#matches}
 * succeeds wins. When no rule matches, the configured default group is returned, so every query
 * always resolves to a group.</p>
 *
 * <p>Instances are immutable; hot-reload is achieved by swapping in a new selector built from
 * updated configuration (the durable/hot-reload config backend is a separate concern).</p>
 */
public final class ResourceGroupSelector {

    private final List<ResourceGroupRule> rules;
    private final String defaultGroup;

    /** Creates a selector from ordered rules and the fallback default group. */
    public ResourceGroupSelector(List<ResourceGroupRule> rules, String defaultGroup) {
        this.rules = List.copyOf(Objects.requireNonNull(rules, "rules must not be null"));
        this.defaultGroup = Objects.requireNonNull(defaultGroup, "defaultGroup must not be null");
        if (defaultGroup.isBlank()) {
            throw new IllegalArgumentException("defaultGroup must not be blank");
        }
    }

    /**
     * Resolves the resource group for a query.
     *
     * @param ctx the query selection context
     * @return the first matching rule's group, or the default group if none match
     */
    public String select(ResourceGroupSelectionContext ctx) {
        Objects.requireNonNull(ctx, "ctx must not be null");
        for (ResourceGroupRule rule : rules) {
            if (rule.matches(ctx)) {
                return rule.resourceGroup();
            }
        }
        return defaultGroup;
    }

    public String defaultGroup() {
        return defaultGroup;
    }

    public List<ResourceGroupRule> rules() {
        return rules;
    }
}
