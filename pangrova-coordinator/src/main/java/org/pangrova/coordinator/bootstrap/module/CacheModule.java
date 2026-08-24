package org.pangrova.coordinator.bootstrap.module;

import org.pangrova.coordinator.query.execution.dispatch.listener.CacheInvalidationListener;
import org.pangrova.coordinator.query.execution.phase.CacheLookupPhase;
import org.pangrova.coordinator.query.execution.result.QueryCacheService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Optional cache module (priority 50).
 * Adds the cache lookup phase and cache invalidation listener when a
 * {@link QueryCacheService} binding is available. Silently skips
 * configuration if no cache service has been bound.
 */
public final class CacheModule implements CoordinatorModule {

    private static final Logger LOG = LoggerFactory.getLogger(CacheModule.class);

    @Override
    public void configure(CoordinatorModuleContext context) {
        QueryCacheService cacheService = context.optional(QueryCacheService.class);
        if (cacheService == null) {
            LOG.info("event=module_skipped module=Cache reason=no_cache_service");
            return;
        }
        context.addQueryPhase(new CacheLookupPhase(cacheService));
        context.addCompletionListener(new CacheInvalidationListener(cacheService));
        LOG.info("event=module_configured module=Cache");
    }

    @Override
    public int priority() {
        return 50;
    }
}
