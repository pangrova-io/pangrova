package org.pangrova.coordinator.query.resourcegroup;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Optional;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Validates rule-based query routing (T07 / G-RES-05): first-match-wins selection, wildcard
 * semantics for unspecified criteria, client-tag subset matching, and default fallthrough.
 */
class ResourceGroupSelectorTest {

    private static ResourceGroupSelectionContext ctx(String user, String source,
                                                     Set<String> tags, String queryType) {
        return new ResourceGroupSelectionContext(
                Optional.ofNullable(user), Optional.ofNullable(source),
                tags, Optional.ofNullable(queryType), Optional.empty(), Optional.empty());
    }

    @Test
    void firstMatchingRuleWins() {
        ResourceGroupSelector selector = new ResourceGroupSelector(List.of(
                ResourceGroupRule.toGroup("etl").user("svc_.*").build(),
                ResourceGroupRule.toGroup("adhoc").user(".*").build()),
                "default");

        assertThat(selector.select(ctx("svc_loader", null, Set.of(), null))).isEqualTo("etl");
        assertThat(selector.select(ctx("alice", null, Set.of(), null))).isEqualTo("adhoc");
    }

    @Test
    void unspecifiedCriteriaAreWildcards() {
        // Rule constrains only queryType; user/source/etc. match anything.
        ResourceGroupSelector selector = new ResourceGroupSelector(List.of(
                ResourceGroupRule.toGroup("writes").queryType("INSERT").build()),
                "reads");

        assertThat(selector.select(ctx("anyone", "anyapp", Set.of(), "INSERT"))).isEqualTo("writes");
        assertThat(selector.select(ctx("anyone", "anyapp", Set.of(), "SELECT"))).isEqualTo("reads");
    }

    @Test
    void clientTagsMatchBySubsetContainment() {
        ResourceGroupSelector selector = new ResourceGroupSelector(List.of(
                ResourceGroupRule.toGroup("priority").tags(Set.of("dashboard")).build()),
                "default");

        // Context carries a superset of the required tags -> matches.
        assertThat(selector.select(ctx("u", null, Set.of("dashboard", "team-a"), null)))
                .isEqualTo("priority");
        // Missing the required tag -> falls through to default.
        assertThat(selector.select(ctx("u", null, Set.of("team-a"), null))).isEqualTo("default");
    }

    @Test
    void multipleCriteriaAllMustMatch() {
        ResourceGroupSelector selector = new ResourceGroupSelector(List.of(
                ResourceGroupRule.toGroup("etl-insert")
                        .user("svc_.*").queryType("INSERT").build()),
                "default");

        assertThat(selector.select(ctx("svc_x", null, Set.of(), "INSERT"))).isEqualTo("etl-insert");
        // User matches but query type does not -> no match.
        assertThat(selector.select(ctx("svc_x", null, Set.of(), "SELECT"))).isEqualTo("default");
    }

    @Test
    void queryTypeAndDatabaseAreCaseInsensitive() {
        ResourceGroupSelector selector = new ResourceGroupSelector(List.of(
                ResourceGroupRule.toGroup("w").queryType("insert").database("Sales").build()),
                "default");

        ResourceGroupSelectionContext c = new ResourceGroupSelectionContext(
                Optional.empty(), Optional.empty(), Set.of(),
                Optional.of("INSERT"), Optional.of("sales"), Optional.empty());
        assertThat(selector.select(c)).isEqualTo("w");
    }

    @Test
    void sourceIpRegexMatches() {
        ResourceGroupSelector selector = new ResourceGroupSelector(List.of(
                ResourceGroupRule.toGroup("internal").sourceIp("10\\.0\\..*").build()),
                "external");

        ResourceGroupSelectionContext internal = new ResourceGroupSelectionContext(
                Optional.empty(), Optional.empty(), Set.of(), Optional.empty(),
                Optional.empty(), Optional.of("10.0.5.7"));
        ResourceGroupSelectionContext external = new ResourceGroupSelectionContext(
                Optional.empty(), Optional.empty(), Set.of(), Optional.empty(),
                Optional.empty(), Optional.of("203.0.113.1"));

        assertThat(selector.select(internal)).isEqualTo("internal");
        assertThat(selector.select(external)).isEqualTo("external");
    }

    @Test
    void noRulesAlwaysReturnsDefault() {
        ResourceGroupSelector selector = new ResourceGroupSelector(List.of(), "default");
        assertThat(selector.select(ResourceGroupSelectionContext.forUser("anyone")))
                .isEqualTo("default");
    }

    @Test
    void constrainedCriterionWithAbsentContextValueDoesNotMatch() {
        ResourceGroupSelector selector = new ResourceGroupSelector(List.of(
                ResourceGroupRule.toGroup("g").user("alice").build()),
                "default");
        // Context has no user -> the user-constrained rule cannot match.
        assertThat(selector.select(ctx(null, null, Set.of(), null))).isEqualTo("default");
    }

    @Test
    void ruleRejectsBlankResourceGroup() {
        assertThatThrownBy(() -> ResourceGroupRule.toGroup("  ").build())
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void selectorRejectsBlankDefaultGroup() {
        assertThatThrownBy(() -> new ResourceGroupSelector(List.of(), " "))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
