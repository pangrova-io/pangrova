package org.pangrova.coordinator.bootstrap;

import org.pangrova.coordinator.bootstrap.module.CacheModule;
import org.pangrova.coordinator.bootstrap.module.CoordinatorModule;
import org.pangrova.coordinator.bootstrap.module.DefaultModuleContext;
import org.pangrova.coordinator.bootstrap.module.IndexingModule;
import org.pangrova.coordinator.bootstrap.module.QueryExecutionModule;
import org.pangrova.coordinator.bootstrap.module.SecurityModule;
import org.pangrova.coordinator.metadata.MetadataBootstrap;
import org.pangrova.coordinator.query.execution.result.QueryCacheService;
import org.pangrova.metadata.catalog.CatalogRegistry;
import org.pangrova.planner.SqlCompiler;
import org.pangrova.resource.scheduler.WeightedFairScheduler;
import org.pangrova.spi.security.AccessControl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

/** Loads query modules and seeds their shared planner and index bindings. */
final class QueryModuleLoader {

    private static final Logger LOG = LoggerFactory.getLogger(QueryModuleLoader.class);

    private final CoordinatorBootstrapConfig config;
    private final MetadataBootstrap metadataBootstrap;
    private final ModuleIntegrationBootstrap moduleWiring;
    private final AccessControl accessControl;
    private final org.pangrova.materialization.usecase.ReaderPinLeases readerPins;

    QueryModuleLoader(
            CoordinatorBootstrapConfig config,
            MetadataBootstrap metadataBootstrap,
            ModuleIntegrationBootstrap moduleWiring,
            AccessControl accessControl) {
        this(config, metadataBootstrap, moduleWiring, accessControl, null);
    }

    QueryModuleLoader(
            CoordinatorBootstrapConfig config,
            MetadataBootstrap metadataBootstrap,
            ModuleIntegrationBootstrap moduleWiring,
            AccessControl accessControl,
            org.pangrova.materialization.usecase.ReaderPinLeases readerPins) {
        this.config = config;
        this.metadataBootstrap = metadataBootstrap;
        this.moduleWiring = moduleWiring;
        this.accessControl = accessControl;
        this.readerPins = readerPins;
    }

    DefaultModuleContext load() {
        List<CoordinatorModule> modules = buildModuleList();
        modules.sort(Comparator.comparingInt(CoordinatorModule::priority));
        DefaultModuleContext context = new DefaultModuleContext();
        seedBindings(context);
        try {
            for (CoordinatorModule module : modules) {
                module.configure(context);
                LOG.debug("event=module_loaded module={} priority={}",
                        module.getClass().getSimpleName(), module.priority());
            }
        } catch (RuntimeException | Error failure) {
            CoordinatorResourceRelease.close(failure, CoordinatorResourceRelease.present(
                    context.optional(WeightedFairScheduler.class), WeightedFairScheduler::close));
            throw failure;
        }
        return context;
    }

    private List<CoordinatorModule> buildModuleList() {
        List<CoordinatorModule> modules = new ArrayList<>();
        boolean tenantAware = moduleWiring != null
                && moduleWiring.getTenantResolver() != null;
        modules.add(new QueryExecutionModule(tenantAware));
        modules.add(new SecurityModule(accessControl));
        modules.add(new CacheModule());
        modules.add(new IndexingModule());
        return modules;
    }

    private void seedBindings(DefaultModuleContext context) {
        var registry = new org.pangrova.planner.runtimefilter.RuntimeFilterRegistry();
        SqlCompiler compiler = new PlannerWiring(config, metadataBootstrap, readerPins, accessControl)
                .buildSqlCompiler(registry);
        context.bind(SqlCompiler.class, compiler);
        context.bind(org.pangrova.planner.runtimefilter.RuntimeFilterRegistry.class, registry);
        CatalogRegistry catalog = metadataBootstrap == null
                ? null : metadataBootstrap.catalogRegistry();
        if (catalog != null) {
            context.bind(CatalogRegistry.class, catalog);
        }
        bindIndexBootstrap(context);
        QueryCacheService cacheService = buildCacheService(compiler);
        if (cacheService != null) {
            context.bind(QueryCacheService.class, cacheService);
        }
    }

    private void bindIndexBootstrap(DefaultModuleContext context) {
        if (moduleWiring == null) {
            return;
        }
        if (moduleWiring.getIndexingBootstrap() != null) {
            context.bind(IndexingBootstrap.class, moduleWiring.getIndexingBootstrap());
        } else if (moduleWiring.getIndexQueryBootstrap() != null) {
            context.bind(IndexQueryBootstrap.class, moduleWiring.getIndexQueryBootstrap());
        }
    }

    private static QueryCacheService buildCacheService(SqlCompiler compiler) {
        var cache = QueryCacheService.resolveCache(compiler);
        if (cache == null) {
            return null;
        }
        var coordinator = new org.pangrova.coordinator.query.cache.ResultCacheCoordinator(
                cache, null, null);
        return new QueryCacheService(coordinator);
    }
}
