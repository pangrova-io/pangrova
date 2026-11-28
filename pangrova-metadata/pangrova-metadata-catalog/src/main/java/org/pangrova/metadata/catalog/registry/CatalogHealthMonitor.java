package org.pangrova.metadata.catalog.registry;

import org.pangrova.common.concurrency.ManagedExecutorFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.pangrova.metadata.api.catalog.CatalogHandle;
import org.pangrova.metadata.api.catalog.CatalogHealthStatus;
import org.pangrova.metadata.api.enums.CatalogState;

/**
 * Periodic health checker for registered catalogs.
 * Transitions catalogs to DEGRADED after consecutive failures and recovers on success.
 */
public class CatalogHealthMonitor {

    private static final Logger LOG = LoggerFactory.getLogger(CatalogHealthMonitor.class);
    private static final int DEGRADED_THRESHOLD = 3;
    private static final long CHECK_INTERVAL_SECONDS = 30;

    private final Map<String, CatalogHandle> catalogs;
    private final Map<String, Integer> failureCounts = new ConcurrentHashMap<>();
    private final ScheduledExecutorService scheduler;

    /** Creates a monitor over the registry's live catalog membership. */
    public CatalogHealthMonitor(Map<String, CatalogHandle> catalogs) {
        // Keep the registry's live membership while preventing monitor-side structural mutation.
        this.catalogs = java.util.Collections.unmodifiableMap(java.util.Objects.requireNonNull(catalogs, "catalogs"));
        ScheduledExecutorService pool =
                ManagedExecutorFactory.newSingleThreadScheduledExecutor("catalog-health-monitor");
        if (pool instanceof ScheduledThreadPoolExecutor stpe) {
            stpe.setRemoveOnCancelPolicy(true);
        }
        this.scheduler = pool;
    }

    public void start() {
        scheduler.scheduleAtFixedRate(
                this::checkAll, CHECK_INTERVAL_SECONDS, CHECK_INTERVAL_SECONDS, TimeUnit.SECONDS);
    }

    /**
     * Shuts down the health-check scheduler.
     * Idempotent: calling multiple times produces no error.
     */
    public void stop() {
        scheduler.shutdown();
    }

    private void checkAll() {
        catalogs.forEach(this::checkCatalog);
    }

    private void checkCatalog(String name, CatalogHandle handle) {
        CatalogHealthStatus status = handle.checkHealth();
        if (status.isHealthy()) {
            handleHealthy(name, handle);
        } else {
            handleUnhealthy(name, handle);
        }
    }

    private void handleHealthy(String name, CatalogHandle handle) {
        failureCounts.remove(name);
        if (handle.getState() == CatalogState.DEGRADED) {
            handle.setState(CatalogState.ACTIVE);
            LOG.info("Catalog recovered, catalog={}, newState=ACTIVE", name);
        }
    }

    private void handleUnhealthy(String name, CatalogHandle handle) {
        int count = failureCounts.merge(name, 1, Integer::sum);
        if (count >= DEGRADED_THRESHOLD && handle.getState() == CatalogState.ACTIVE) {
            handle.setState(CatalogState.DEGRADED);
            LOG.warn("Catalog degraded, catalog={}, failureCount={}", name, count);
        }
    }
}
