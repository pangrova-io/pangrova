package org.pangrova.coordinator.dynamictable;

import org.junit.jupiter.api.Test;
import org.pangrova.coordinator.testinfra.DynamicTablePublicationFixtures;
import org.pangrova.planner.mv.DynamicTableRewriter;
import org.pangrova.planner.parse.PangrovaParserFactory;
import org.pangrova.planner.rewrite.DynamicTableSqlRewriter;
import org.pangrova.connector.spi.metadata.table.QualifiedTableName;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** Tenant-scoped resolution against the unified published-materialization read model. */
class DurableDynamicTableResolverTest {

    @Test
    void sameSimpleNameInTwoSchemasKeepsIndependentPhysicalAndLogicalIdentity() throws Exception {
        var catalog = DynamicTablePublicationFixtures.catalog();
        var analytics = DynamicTablePublicationFixtures.create(catalog, "tenant",
                "lake.analytics.rollup", "SELECT 1");
        var reporting = DynamicTablePublicationFixtures.create(catalog, "tenant",
                "lake.reporting.rollup", "SELECT 1");
        var resolver = new DurableDynamicTableResolver(catalog, "tenant", "lake", "analytics");

        var local = resolver.resolve(List.of("rollup")).orElseThrow();
        assertThat(local.logical()).isEqualTo(new QualifiedTableName("lake", "analytics", "rollup"));
        assertThat(local.physical().toString())
                .isEqualTo(analytics.physicalTable().qualifiedName());
        var other = resolver.resolve(List.of("reporting", "rollup")).orElseThrow();
        assertThat(other.logical()).isEqualTo(new QualifiedTableName("lake", "reporting", "rollup"));
        assertThat(other.physical().toString())
                .isEqualTo(reporting.physicalTable().qualifiedName());
        assertThat(resolver.resolve(List.of("lake.reporting", "rollup"))).isEmpty();
        assertThat(resolver.resolve(List.of("other", "analytics", "rollup"))).isEmpty();
        assertThat(new DurableDynamicTableResolver(catalog, "other-tenant", "lake", "analytics")
                .resolve(List.of("rollup"))).isEmpty();

        var sqlRewriter = new DynamicTableSqlRewriter(new PangrovaParserFactory(),
                new DynamicTableRewriter(resolver));
        var normal = sqlRewriter.rewriteWithAuthority("SELECT id FROM lake.analytics.rollup");
        assertThat(normal.physicalToLogical()).containsEntry(local.physical(), local.logical());
        var published = DynamicTablePublicationFixtures.refresh(catalog, analytics);
        var refreshed = sqlRewriter.rewriteWithAuthority("SELECT id FROM lake.analytics.rollup");
        assertThat(refreshed.sql()).contains(published.physicalTable().qualifiedName());
        assertThat(refreshed.physicalToLogical()).containsEntry(
                new QualifiedTableName("lake", published.physicalTable().schema(),
                        published.physicalTable().table()), local.logical());
        for (String quoted : List.of("SELECT id FROM lake.analytics.\"Rollup\"",
                "SELECT id FROM \"lake.analytics\".rollup")) {
            var rejected = sqlRewriter.rewriteWithAuthority(quoted);
            assertThat(rejected.sql()).isEqualTo(quoted);
            assertThat(rejected.physicalToLogical()).isEmpty();
        }
    }

    @Test
    void leasesEveryPublishedGenerationExposedToPlanning() {
        var catalog = DynamicTablePublicationFixtures.catalog();
        var dynamicTable = DynamicTablePublicationFixtures.create(catalog, "tenant",
                "lake.analytics.rollup", "SELECT 1");
        var published = DynamicTablePublicationFixtures.refresh(catalog, dynamicTable);
        var pinned = new java.util.ArrayList<
                org.pangrova.materialization.domain.GenerationId>();
        var resolver = new DurableDynamicTableResolver(catalog, "tenant", "lake", "analytics",
                resolution -> pinned.add(resolution.generation()));

        var target = resolver.resolve(List.of("rollup")).orElseThrow();

        assertThat(target.physical().toString())
                .isEqualTo(published.physicalTable().qualifiedName());
        assertThat(pinned).containsExactly(published.generation());
    }

}
