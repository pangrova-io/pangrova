package org.pangrova.planner.core.analysis;

import org.pangrova.connector.api.metadata.table.QualifiedTableName;

import org.apache.calcite.jdbc.CalciteSchema;
import org.junit.jupiter.api.Test;
import org.pangrova.planner.core.cache.QueryResultCache;
import org.pangrova.planner.core.mv.DynamicTableRewriter;
import org.pangrova.planner.core.optimization.MaterializedViewRewritePass;
import org.pangrova.planner.core.parse.PangrovaParserFactory;
import org.pangrova.planner.core.rewrite.DynamicTableSqlRewriter;
import org.pangrova.planner.core.runtimefilter.RuntimeFilter;
import org.pangrova.planner.core.runtimefilter.RuntimeFilterRegistry;
import org.pangrova.planner.core.statistics.PlannerTableStatistics;
import org.pangrova.planner.core.statistics.StatisticsCollector;
import org.pangrova.planner.core.substrait.SubstraitConverter;
import org.pangrova.planner.core.substrait.SubstraitPlanSerializer;

import java.time.Instant;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class PlannerServiceOwnershipTest {
    @Test
    void contextAndCompilerBorrowTheSameLiveServicesNotIndependentSnapshots() throws Exception {
        var statistics = new StatisticsCollector();
        var filters = new RuntimeFilterRegistry();
        var dynamic = new DynamicTableRewriter(parts -> java.util.Optional.empty());
        try (var cache = new QueryResultCache(null)) {
            var context = new PlannerContext(statistics, null, cache, null,
                    filters, dynamic, null, null, null);
            var planner = new PangrovaPlanner(CalciteSchema.createRootSchema(false), List.of(""), context);
            var compiler = new SqlCompiler(planner, new SubstraitConverter(), new SubstraitPlanSerializer());
            assertThat(context.queryResultCache()).isSameAs(cache);
            assertThat(planner.getQueryResultCache()).isSameAs(cache);
            assertThat(compiler.queryResultCache()).isSameAs(cache);
            assertThat(context.statisticsCollector()).isSameAs(statistics);
            var observed = new PlannerTableStatistics(new QualifiedTableName("test", "db", "documents"), Map.of(), Instant.EPOCH);
            context.statisticsCollector().putStatistics(new QualifiedTableName("test", "db", "documents"), observed);
            assertThat(statistics.getStatistics(new QualifiedTableName("test", "db", "documents"))).contains(observed);
            assertThat(context.runtimeFilterRegistry()).isSameAs(filters);
            var filter = RuntimeFilter.minMax("id", 1, 7);
            context.runtimeFilterRegistry().register("query:stage", filter);
            assertThat(filters.getFilter("query:stage")).contains(filter);
            assertThat(context.dynamicTableRewriter()).isSameAs(dynamic);
        }
    }

    @Test
    void rewritePassesObserveRegistrationsThroughTheOriginalOwner() {
        var filters = new RuntimeFilterRegistry();
        var pass = new MaterializedViewRewritePass(null, null, filters);
        assertThat(pass.getRuntimeFilterRegistry()).isSameAs(filters);
        var filter = RuntimeFilter.minMax("id", 1, 7);
        pass.getRuntimeFilterRegistry().register("query:stage", filter);
        assertThat(filters.getFilter("query:stage")).contains(filter);
        var publications = new MutablePublishedResolver();
        var dynamic = new DynamicTableRewriter(publications);
        var rewrite = new DynamicTableSqlRewriter(new PangrovaParserFactory(), dynamic);
        publications.physical = new QualifiedTableName("lake", "analytics", "__pg_mat_one");
        assertThat(rewrite.rewrite("SELECT id FROM logical")).contains("__pg_mat_one");
        publications.physical = new QualifiedTableName("lake", "analytics", "__pg_mat_two");
        assertThat(rewrite.rewrite("SELECT id FROM logical")).contains("__pg_mat_two");
    }

    @Test
    void dynamicTableRewriteChangesOnlyTheParsedTableReference() {
        var publications = new MutablePublishedResolver();
        publications.physical = new QualifiedTableName("iceberg_catalog", "default", "physical");
        var rewrite = new DynamicTableSqlRewriter(new PangrovaParserFactory(),
                new DynamicTableRewriter(publications));

        assertThat(rewrite.rewrite("SELECT 'logical' AS label FROM logical"))
                .isEqualTo("SELECT 'logical' AS label FROM iceberg_catalog.default.physical");
        assertThat(rewrite.rewrite("SELECT logical FROM logical"))
                .isEqualTo("SELECT logical FROM iceberg_catalog.default.physical");
        assertThat(rewrite.rewrite("SELECT 1 /* logical */ FROM logical"))
                .isEqualTo("SELECT 1 /* logical */ FROM iceberg_catalog.default.physical");
        assertThat(rewrite.rewrite("SELECT * FROM iceberg_catalog.default.logical WHERE id = 1"))
                .isEqualTo("SELECT * FROM iceberg_catalog.default.physical WHERE id = 1");
    }

    private static final class MutablePublishedResolver
            implements DynamicTableRewriter.PublishedTargetResolver {
        private static final QualifiedTableName LOGICAL =
                new QualifiedTableName("iceberg_catalog", "default", "logical");
        private QualifiedTableName physical;

        @Override
        public java.util.Optional<QualifiedTableName> canonicalLogical(List<String> nameParts) {
            return isLogical(nameParts) ? java.util.Optional.of(LOGICAL) : java.util.Optional.empty();
        }

        @Override
        public java.util.Optional<DynamicTableRewriter.PublishedTarget> resolve(
                List<String> nameParts) {
            return isLogical(nameParts)
                    ? java.util.Optional.of(new DynamicTableRewriter.PublishedTarget(LOGICAL, physical))
                    : java.util.Optional.empty();
        }

        private static boolean isLogical(List<String> nameParts) {
            return !nameParts.isEmpty()
                    && nameParts.get(nameParts.size() - 1).equalsIgnoreCase("logical");
        }
    }
}
