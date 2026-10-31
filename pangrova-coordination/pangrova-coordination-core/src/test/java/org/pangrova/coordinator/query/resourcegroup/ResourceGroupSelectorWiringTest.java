package org.pangrova.coordinator.query.resourcegroup;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Verifies that the {@link ResourceGroupSelector} resolves different queries
 * (different users/tags/sources) to different resource groups.
 */
class ResourceGroupSelectorWiringTest {

    /**
     * Two queries from different users resolve to different resource groups.
     */
    @Test
    void differentUsersGetDifferentResourceGroups() {
        ResourceGroupSelector selector = new ResourceGroupSelector(List.of(
                ResourceGroupRule.toGroup("etl").user("svc_.*").build(),
                ResourceGroupRule.toGroup("adhoc").user(".*").build()),
                "default");

        // svc_loader matches first rule -> "etl"
        ResourceGroupSelectionContext etlCtx = new ResourceGroupSelectionContext(
                Optional.of("svc_loader"), Optional.empty(), Set.of(),
                Optional.empty(), Optional.empty(), Optional.empty());
        assertThat(selector.select(etlCtx)).isEqualTo("etl");

        // alice matches second rule -> "adhoc"
        ResourceGroupSelectionContext adhocCtx = new ResourceGroupSelectionContext(
                Optional.of("alice"), Optional.empty(), Set.of(),
                Optional.empty(), Optional.empty(), Optional.empty());
        assertThat(selector.select(adhocCtx)).isEqualTo("adhoc");
    }

    /**
     * Queries with matching client_tags resolve to priority groups.
     */
    @Test
    void clientTagsRouteToCorrectGroup() {
        ResourceGroupSelector selector = new ResourceGroupSelector(List.of(
                ResourceGroupRule.toGroup("interactive")
                        .tags(Set.of("dashboard")).build(),
                ResourceGroupRule.toGroup("batch")
                        .tags(Set.of("etl-job")).build()),
                "default");

        // dashboard tag -> "interactive"
        ResourceGroupSelectionContext dashCtx = new ResourceGroupSelectionContext(
                Optional.empty(), Optional.empty(), Set.of("dashboard", "team-a"),
                Optional.empty(), Optional.empty(), Optional.empty());
        assertThat(selector.select(dashCtx)).isEqualTo("interactive");

        // etl-job tag -> "batch"
        ResourceGroupSelectionContext etlCtx = new ResourceGroupSelectionContext(
                Optional.empty(), Optional.empty(), Set.of("etl-job"),
                Optional.empty(), Optional.empty(), Optional.empty());
        assertThat(selector.select(etlCtx)).isEqualTo("batch");

        // no matching tag -> "default"
        ResourceGroupSelectionContext defaultCtx = new ResourceGroupSelectionContext(
                Optional.empty(), Optional.empty(), Set.of("misc"),
                Optional.empty(), Optional.empty(), Optional.empty());
        assertThat(selector.select(defaultCtx)).isEqualTo("default");
    }

    /**
     * Source and sourceIp session properties are used for routing.
     */
    @Test
    void sourceAndSourceIpAreUsedForRouting() {
        ResourceGroupSelector selector = new ResourceGroupSelector(List.of(
                ResourceGroupRule.toGroup("internal")
                        .sourceIp("10\\.0\\..*").build(),
                ResourceGroupRule.toGroup("external-bi")
                        .source("tableau").build()),
                "default");

        // Internal IP
        ResourceGroupSelectionContext internalCtx = new ResourceGroupSelectionContext(
                Optional.empty(), Optional.empty(), Set.of(),
                Optional.empty(), Optional.empty(), Optional.of("10.0.5.7"));
        assertThat(selector.select(internalCtx)).isEqualTo("internal");

        // Tableau source from external IP
        ResourceGroupSelectionContext biCtx = new ResourceGroupSelectionContext(
                Optional.empty(), Optional.of("tableau"), Set.of(),
                Optional.empty(), Optional.empty(), Optional.of("203.0.1.1"));
        assertThat(selector.select(biCtx)).isEqualTo("external-bi");
    }

    /**
     * When no rule matches, the default group is used.
     */
    @Test
    void noMatchUsesDefaultGroup() {
        ResourceGroupSelector selector = new ResourceGroupSelector(List.of(
                ResourceGroupRule.toGroup("admin").user("admin").build()),
                "default-fallback");

        ResourceGroupSelectionContext ctx = ResourceGroupSelectionContext.forUser("bob");
        assertThat(selector.select(ctx)).isEqualTo("default-fallback");
    }
}
