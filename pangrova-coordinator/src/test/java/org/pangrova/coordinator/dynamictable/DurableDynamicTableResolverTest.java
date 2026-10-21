package org.pangrova.coordinator.dynamictable;

import org.junit.jupiter.api.Test;
import org.pangrova.coordinator.testinfra.SharedPostgresContainer;
import org.pangrova.coordinator.testinfra.DynamicTablePublicationFixtures;
import org.pangrova.metadata.dynamictable.JdbcDynamicTablePublicationStore;
import org.pangrova.planner.mv.DynamicTableRewriter;
import org.pangrova.planner.parse.PangrovaParserFactory;
import org.pangrova.planner.rewrite.DynamicTableSqlRewriter;
import org.pangrova.spi.connector.metadata.table.QualifiedTableName;
import org.pangrova.testing.ContainerTestBase;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** Component-safe, tenant-scoped resolution from the migrated publication authority. */
class DurableDynamicTableResolverTest extends ContainerTestBase {

    @Test
    void sameSimpleNameInTwoSchemasKeepsIndependentPhysicalAndLogicalIdentity() throws Exception {
        var dataSource = SharedPostgresContainer.migratedDataSource("dt_resolver");
        var store = new JdbcDynamicTablePublicationStore(dataSource);
        var analytics = DynamicTablePublicationFixtures.create(dataSource, "tenant", "lake.analytics.rollup",
                "SELECT 1", "lake", "analytics");
        var reporting = DynamicTablePublicationFixtures.create(dataSource, "tenant", "lake.reporting.rollup",
                "SELECT 1", "lake", "reporting");
        var resolver = new DurableDynamicTableResolver(store, "tenant", "lake", "analytics");

        var local = resolver.resolve(List.of("rollup")).orElseThrow();
        assertThat(local.logical()).isEqualTo(new QualifiedTableName("lake", "analytics", "rollup"));
        assertThat(local.physical().toString()).isEqualTo(analytics.candidateTable());
        var other = resolver.resolve(List.of("reporting", "rollup")).orElseThrow();
        assertThat(other.logical()).isEqualTo(new QualifiedTableName("lake", "reporting", "rollup"));
        assertThat(other.physical().toString()).isEqualTo(reporting.candidateTable());
        assertThat(resolver.resolve(List.of("lake.reporting", "rollup"))).isEmpty();
        assertThat(resolver.resolve(List.of("other", "analytics", "rollup"))).isEmpty();
        assertThat(new DurableDynamicTableResolver(store, "other-tenant", "lake", "analytics")
                .resolve(List.of("rollup"))).isEmpty();

        var sqlRewriter = new DynamicTableSqlRewriter(new PangrovaParserFactory(),
                new DynamicTableRewriter(resolver));
        var normal = sqlRewriter.rewriteWithAuthority("SELECT id FROM lake.analytics.rollup");
        assertThat(normal.physicalToLogical()).containsEntry(local.physical(), local.logical());
        var next = store.reserveRefresh(store.load("tenant", "lake.analytics.rollup").orElseThrow(),
                DynamicTablePublicationFixtures.issuer(dataSource));
        var published = DynamicTablePublicationFixtures.publish(dataSource, next.attempt());
        var refreshed = sqlRewriter.rewriteWithAuthority("SELECT id FROM lake.analytics.rollup");
        assertThat(refreshed.sql()).contains(published.candidateTable());
        assertThat(refreshed.physicalToLogical()).containsEntry(
                new QualifiedTableName("lake", "analytics", published.candidateTable().split("\\.")[2]), local.logical());
        for (String quoted : List.of("SELECT id FROM lake.analytics.\"Rollup\"",
                "SELECT id FROM \"lake.analytics\".rollup")) {
            var rejected = sqlRewriter.rewriteWithAuthority(quoted);
            assertThat(rejected.sql()).isEqualTo(quoted);
            assertThat(rejected.physicalToLogical()).isEmpty();
        }
    }

}
