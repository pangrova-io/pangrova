package org.pangrova.coordinator.bootstrap;

import java.time.Duration;
import java.util.List;

import org.pangrova.coordinator.metadata.CatalogStatisticsProvider;
import org.pangrova.coordinator.metadata.MaterializedViewPersistenceListener;
import org.pangrova.coordinator.metadata.MaterializedViewRecovery;
import org.pangrova.coordinator.metadata.MetadataBootstrap;
import org.pangrova.coordinator.query.cache.RedisL2CacheStore;
import org.pangrova.planner.PangrovaPlanner;
import org.pangrova.planner.PlannerContext;
import org.pangrova.planner.SqlCompiler;
import org.pangrova.planner.cache.InMemoryL2CacheStore;
import org.pangrova.planner.cache.L2CacheStore;
import org.pangrova.planner.cache.QueryResultCache;
import org.pangrova.planner.cost.CboConfig;
import org.pangrova.planner.cost.CostModelConfig;
import org.pangrova.planner.mv.MaterializedViewQueryRewriter;
import org.pangrova.planner.mv.MaterializedViewRegistry;
import org.pangrova.planner.runtimefilter.DynamicPartitionPruningConfig;
import org.pangrova.planner.runtimefilter.RuntimeFilterRegistry;
import org.pangrova.planner.stats.StatisticsCollector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Builds the SQL compiler and its metadata-aware planner: catalog statistics, the materialized-view
 * registry (recovered from the metadata store), and the plan-result cache. Extracted from
 * QueryBootstrap to keep bootstrap wiring under the 500-line quality gate.
 */
final class PlannerWiring {

    private static final Logger LOG = LoggerFactory.getLogger(PlannerWiring.class);

    private static final Duration MV_FRESHNESS_WINDOW = Duration.ofMinutes(30);

    private final CoordinatorBootstrapConfig config;
    private final MetadataBootstrap metadataBootstrap;

    PlannerWiring(CoordinatorBootstrapConfig config, MetadataBootstrap metadataBootstrap) {
        this.config = config;
        this.metadataBootstrap = metadataBootstrap;
    }

    /**
     * Builds the compiler. Without a metadata bootstrap the planner has no catalog schema, so it
     * falls back to a bare planner that can still parse and convert but cannot resolve tables.
     */
    SqlCompiler buildSqlCompiler(RuntimeFilterRegistry reg) {
        PangrovaPlanner planner = metadataBootstrap == null
                ? new PangrovaPlanner()
                : buildMetadataAwarePlanner(reg);
        return new SqlCompiler(planner,
                new org.pangrova.planner.substrait.SubstraitConverter(),
                new org.pangrova.planner.substrait.SubstraitPlanSerializer());
    }

    private PangrovaPlanner buildMetadataAwarePlanner(RuntimeFilterRegistry reg) {
        var rootSchema = metadataBootstrap.buildCalciteSchema();
        var stats = new StatisticsCollector(
                new CatalogStatisticsProvider(metadataBootstrap.catalogRegistry()));
        var mvRewriter = new MaterializedViewQueryRewriter(
                buildMaterializedViewRegistry(), MV_FRESHNESS_WINDOW);
        var cache = new QueryResultCache(buildL2CacheStore());
        return new PangrovaPlanner(rootSchema, List.of(""),
                new PlannerContext(stats, mvRewriter, cache,
                        DynamicPartitionPruningConfig.DEFAULT, reg,
                        null, null, CboConfig.DEFAULT, CostModelConfig.SSD_DEFAULT));
    }

    /** Rebuilds the in-memory registry from the persisted definitions before serving queries. */
    private MaterializedViewRegistry buildMaterializedViewRegistry() {
        var store = metadataBootstrap.materializedViewStore();
        var registry = new MaterializedViewRegistry(new MaterializedViewPersistenceListener(store));
        new MaterializedViewRecovery(store, registry).recover();
        return registry;
    }

    private L2CacheStore buildL2CacheStore() {
        var cacheConfig = config.cacheConfig();
        if (!cacheConfig.isRedisEnabled()) {
            return new InMemoryL2CacheStore();
        }
        LOG.info("event=redis_l2_cache_enabled url={} ttl={} maxEntry={}",
                cacheConfig.redisUrl(), cacheConfig.ttlSeconds(), cacheConfig.maxEntryBytes());
        return new RedisL2CacheStore(
                cacheConfig.redisUrl(), cacheConfig.ttlSeconds(), cacheConfig.maxEntryBytes());
    }
}
