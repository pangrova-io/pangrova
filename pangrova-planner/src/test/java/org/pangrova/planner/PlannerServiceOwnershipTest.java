package org.pangrova.planner;

import org.apache.calcite.jdbc.CalciteSchema;
import org.junit.jupiter.api.Test;
import org.pangrova.planner.cache.QueryResultCache;
import org.pangrova.planner.mv.DynamicTableRewriter;
import org.pangrova.planner.mv.MaterializedViewRegistry;
import org.pangrova.planner.optimization.MaterializedViewRewritePass;
import org.pangrova.planner.parse.PangrovaParserFactory;
import org.pangrova.planner.rewrite.DynamicTableSqlRewriter;
import org.pangrova.planner.runtimefilter.RuntimeFilter;
import org.pangrova.planner.runtimefilter.RuntimeFilterRegistry;
import org.pangrova.planner.stats.PlannerTableStatistics;
import org.pangrova.planner.stats.StatisticsCollector;
import org.pangrova.planner.substrait.SubstraitConverter;
import org.pangrova.planner.substrait.SubstraitPlanSerializer;

import java.time.Instant;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class PlannerServiceOwnershipTest {
    @Test
    void contextAndCompilerBorrowTheSameLiveServicesNotIndependentSnapshots() throws Exception {
        var statistics = new StatisticsCollector();
        var filters = new RuntimeFilterRegistry();
        var dynamic = new DynamicTableRewriter(new MaterializedViewRegistry());
        try (var cache = new QueryResultCache(null)) {
            var context = new PlannerContext(statistics, null, cache, null,
                    filters, dynamic, null, null, null);
            var planner = new PangrovaPlanner(CalciteSchema.createRootSchema(false), List.of(""), context);
            var compiler = new SqlCompiler(planner, new SubstraitConverter(), new SubstraitPlanSerializer());
            assertThat(context.queryResultCache()).isSameAs(cache);
            assertThat(planner.getQueryResultCache()).isSameAs(cache);
            assertThat(compiler.queryResultCache()).isSameAs(cache);
            assertThat(context.statisticsCollector()).isSameAs(statistics);
            var observed = new PlannerTableStatistics("documents", Map.of(), Instant.EPOCH);
            context.statisticsCollector().putStatistics("documents", observed);
            assertThat(statistics.getStatistics("documents")).contains(observed);
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
        var definitions = new MaterializedViewRegistry();
        definitions.register(new org.pangrova.planner.mv.MaterializedViewDefinition(
                "logical", "SELECT id FROM source", org.pangrova.parser.ast.RefreshStrategy.INCREMENTAL,
                true, java.util.Optional.of(Instant.EPOCH), Instant.EPOCH,
                java.util.Set.of(), java.util.Set.of(), java.util.Set.of()));
        var dynamic = new DynamicTableRewriter(definitions);
        var rewrite = new DynamicTableSqlRewriter(new PangrovaParserFactory(), dynamic);
        assertThat(rewrite.getDynamicTableRewriter()).isSameAs(dynamic);
        dynamic.registerTargetOverride("logical", "physical");
        assertThat(rewrite.rewrite("SELECT id FROM logical")).containsIgnoringCase("physical");
        dynamic.removeTargetOverride("logical");
        assertThat(rewrite.rewrite("SELECT id FROM logical")).containsIgnoringCase("logical");
    }
}
