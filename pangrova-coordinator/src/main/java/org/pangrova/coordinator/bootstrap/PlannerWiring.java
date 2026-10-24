package org.pangrova.coordinator.bootstrap;

import java.util.List;

import org.pangrova.coordinator.metadata.read.CatalogStatisticsProvider;
import org.pangrova.coordinator.metadata.MetadataBootstrap;
import org.pangrova.coordinator.materialization.KernelMaterializedViewRewriteCatalog;
import org.pangrova.coordinator.query.cache.RedisL2CacheStore;
import org.pangrova.planner.PangrovaPlanner;
import org.pangrova.planner.PlannerContext;
import org.pangrova.planner.SqlCompiler;
import org.pangrova.planner.cache.InMemoryL2CacheStore;
import org.pangrova.planner.cache.L2CacheStore;
import org.pangrova.planner.cache.QueryResultCache;
import org.pangrova.planner.cost.CboConfig;
import org.pangrova.planner.cost.CostModelConfig;
import org.pangrova.planner.mv.DynamicTableRewriter;
import org.pangrova.planner.mv.MaterializedViewRewriteCatalog;
import org.pangrova.coordinator.dynamictable.DurableDynamicTableResolver;
import org.pangrova.planner.runtimefilter.DynamicPartitionPruningConfig;
import org.pangrova.planner.runtimefilter.RuntimeFilterRegistry;
import org.pangrova.planner.stats.StatisticsCollector;
import org.pangrova.spi.security.AccessControl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Builds the SQL compiler and its metadata-aware planner: catalog statistics, the materialized-view
 * registry (recovered from the metadata store), and the plan-result cache. Extracted from
 * QueryBootstrap to keep bootstrap wiring under the 500-line quality gate.
 */
final class PlannerWiring {

    private static final Logger LOG = LoggerFactory.getLogger(PlannerWiring.class);

    private final CoordinatorBootstrapConfig config;
    private final MetadataBootstrap metadataBootstrap;
    private final org.pangrova.materialization.usecase.ReaderPinLeases readerPins;
    private final AccessControl accessControl;

    PlannerWiring(CoordinatorBootstrapConfig config, MetadataBootstrap metadataBootstrap) {
        this(config, metadataBootstrap, null, null);
    }

    PlannerWiring(CoordinatorBootstrapConfig config, MetadataBootstrap metadataBootstrap,
            org.pangrova.materialization.usecase.ReaderPinLeases readerPins) {
        this(config, metadataBootstrap, readerPins, null);
    }

    PlannerWiring(CoordinatorBootstrapConfig config, MetadataBootstrap metadataBootstrap,
            org.pangrova.materialization.usecase.ReaderPinLeases readerPins,
            AccessControl accessControl) {
        this.config = config;
        this.metadataBootstrap = metadataBootstrap;
        this.readerPins = readerPins;
        this.accessControl = accessControl;
    }

    /**
     * Builds the compiler. Without a metadata bootstrap the planner has no catalog schema, so it
     * falls back to a bare planner that can still parse and convert but cannot resolve tables.
     */
    SqlCompiler buildSqlCompiler(RuntimeFilterRegistry reg) {
        SqlCompiler compiler = new SqlCompiler(new PangrovaPlanner(),
                new org.pangrova.planner.substrait.SubstraitConverter(),
                new org.pangrova.planner.substrait.SubstraitPlanSerializer());
        if (metadataBootstrap == null) {
            return compiler;
        }
        var cache = new QueryResultCache(buildL2CacheStore());
        return compiler.withQueryResultCache(cache).withSessionPlanner(
                session -> buildMetadataAwarePlanner(reg, session, cache));
    }

    private PangrovaPlanner buildMetadataAwarePlanner(RuntimeFilterRegistry reg,
            org.pangrova.spi.connector.ConnectorSession session, QueryResultCache cache) {
        var readBindings = new org.pangrova.coordinator.metadata.read.PlanningReadBindings(
                metadataBootstrap.catalogRegistry(), session);
        var rootSchema = metadataBootstrap.buildCalciteSchema(session, readBindings);
        var stats = new StatisticsCollector(
                new CatalogStatisticsProvider(readBindings));
        var mvCatalog = buildMaterializedViewRewriteCatalog(session);
        var path = session.getCatalog().map(catalog -> session.getSchema()
                .map(schema -> List.of(catalog, schema)).orElse(List.of(catalog))).orElse(List.of(""));
        DynamicTableRewriter dynamicTableRewriter = null;
        var materializations = metadataBootstrap.materializationCatalog();
        if (materializations != null) {
            String tenantId = session.getProperties().getOrDefault("tenant_id", "default");
            java.util.function.Consumer<
                    org.pangrova.materialization.port.PublishedMaterialization> pinner =
                    readerPin(session);
            dynamicTableRewriter = new DynamicTableRewriter(new DurableDynamicTableResolver(
                    materializations, tenantId, session.getCatalog().orElse(null),
                    session.getSchema().orElse(null), pinner),
                    DynamicTableRewriter.isInternalExecution(session));
        }
        return new PangrovaPlanner(rootSchema, path,
                new PlannerContext(stats, mvCatalog, cache,
                        DynamicPartitionPruningConfig.DEFAULT, reg,
                        dynamicTableRewriter, null, CboConfig.DEFAULT, CostModelConfig.SSD_DEFAULT));
    }

    /**
     * Rewrite candidates are per session because authorization, tenant and default
     * schema all participate; the catalog is cheap and reads published state lazily.
     */
    private MaterializedViewRewriteCatalog buildMaterializedViewRewriteCatalog(
            org.pangrova.spi.connector.ConnectorSession session) {
        var materializations = metadataBootstrap.materializationCatalog();
        if (materializations == null) {
            return null;
        }
        String tenantId = session.getProperties().getOrDefault("tenant_id", "default");
        return KernelMaterializedViewRewriteCatalog.create(materializations, tenantId, session,
                accessControl, java.time.Instant.now(), readerPin(session)).orElse(null);
    }

    /** Leases every published generation this admitted query resolves for execution. */
    private java.util.function.Consumer<
            org.pangrova.materialization.port.PublishedMaterialization> readerPin(
                    org.pangrova.spi.connector.ConnectorSession session) {
        var authority = session.getQueryAuthority().orElse(null);
        if (readerPins == null || authority == null) {
            return published -> { };
        }
        return published -> readerPins.acquire(published.generation(),
                new org.pangrova.materialization.domain.QueryId(authority.queryId()),
                session.getUser());
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
