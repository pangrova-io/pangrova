package org.pangrova.coordinator.dynamictable;

import org.junit.jupiter.api.Test;
import org.pangrova.coordinator.testinfra.SharedPostgresContainer;
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
        var store = new JdbcDynamicTablePublicationStore(
                SharedPostgresContainer.migratedDataSource("dt_resolver"));
        publish(store, "lake.analytics.rollup", "lake.analytics.__pg_dt_analytics");
        publish(store, "lake.reporting.rollup", "lake.reporting.__pg_dt_reporting");
        var resolver = new DurableDynamicTableResolver(store, "tenant", "lake", "analytics");

        var local = resolver.resolve(List.of("rollup")).orElseThrow();
        assertThat(local.logical()).isEqualTo(new QualifiedTableName("lake", "analytics", "rollup"));
        assertThat(local.physical()).isEqualTo(new QualifiedTableName(
                "lake", "analytics", "__pg_dt_analytics"));
        var other = resolver.resolve(List.of("reporting", "rollup")).orElseThrow();
        assertThat(other.logical()).isEqualTo(new QualifiedTableName("lake", "reporting", "rollup"));
        assertThat(other.physical()).isEqualTo(new QualifiedTableName(
                "lake", "reporting", "__pg_dt_reporting"));
        assertThat(resolver.resolve(List.of("lake.reporting", "rollup"))).isEmpty();
        assertThat(resolver.resolve(List.of("other", "analytics", "rollup"))).isEmpty();
        assertThat(new DurableDynamicTableResolver(store, "other-tenant", "lake", "analytics")
                .resolve(List.of("rollup"))).isEmpty();

        var sqlRewriter = new DynamicTableSqlRewriter(new PangrovaParserFactory(),
                new DynamicTableRewriter(resolver));
        var normal = sqlRewriter.rewriteWithAuthority("SELECT id FROM lake.analytics.rollup");
        assertThat(normal.physicalToLogical()).containsEntry(local.physical(), local.logical());
        store.publish(store.load("tenant", "lake.analytics.rollup").orElseThrow(),
                "lake.analytics.__pg_dt_new_head");
        var refreshed = sqlRewriter.rewriteWithAuthority("SELECT id FROM lake.analytics.rollup");
        assertThat(refreshed.sql()).contains("lake.analytics.__pg_dt_new_head");
        assertThat(refreshed.physicalToLogical()).containsEntry(
                new QualifiedTableName("lake", "analytics", "__pg_dt_new_head"), local.logical());
        for (String quoted : List.of("SELECT id FROM lake.analytics.\"Rollup\"",
                "SELECT id FROM \"lake.analytics\".rollup")) {
            var rejected = sqlRewriter.rewriteWithAuthority(quoted);
            assertThat(rejected.sql()).isEqualTo(quoted);
            assertThat(rejected.physicalToLogical()).isEmpty();
        }
    }

    private static void publish(JdbcDynamicTablePublicationStore store, String logical,
            String physical) {
        store.create("tenant", logical, "SELECT 1", "lake", "analytics");
        store.publish(store.load("tenant", logical).orElseThrow(), physical);
    }
}
