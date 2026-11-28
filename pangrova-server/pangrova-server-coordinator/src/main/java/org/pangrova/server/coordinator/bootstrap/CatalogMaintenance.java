package org.pangrova.server.coordinator.bootstrap;

import org.pangrova.common.concurrency.ExecutorDrain;
import org.pangrova.common.concurrency.ManagedExecutorFactory;
import org.pangrova.metadata.api.catalog.CatalogHandle;
import org.pangrova.metadata.catalog.registry.CatalogRegistry;
import org.pangrova.metadata.outbox.MetadataSyncService;
import org.pangrova.metadata.outbox.TableVersionObservationReport;
import org.pangrova.metadata.outbox.TableVersionObserver;
import org.pangrova.persistence.catalog.postgresql.invalidation.PostgresInvalidationPoller;
import org.pangrova.common.lifecycle.ResourceCloser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/** Owns the coordinator's catalog sync and invalidation schedules across leadership terms. */
final class CatalogMaintenance implements AutoCloseable {
    private static final Logger LOG = LoggerFactory.getLogger(CatalogMaintenance.class);
    private static final long METADATA_SYNC_INTERVAL_SECONDS = 60;

    private static final long INVALIDATION_CLEANUP_INTERVAL_HOURS = 24;

    // Metadata sync (leader-gated periodic sync for federation catalogs)
    private MetadataSyncService metadataSyncService;
    private TableVersionObserver versionObserver;
    private CatalogRegistry catalogRegistry;
    private ScheduledExecutorService metadataSyncScheduler;
    private volatile ScheduledFuture<?> metadataSyncTask;

    // Invalidation log cleanup (leader-gated)
    private PostgresInvalidationPoller invalidationPoller;
    private ScheduledFuture<?> invalidationCleanupTask;
    private ScheduledExecutorService invalidationCleanupScheduler;

    CatalogMaintenance() {
        this(ManagedExecutorFactory.newSingleThreadScheduledExecutor("invalidation-cleanup"));
    }

    /** Owns the supplied executor for the process lifetime, across leadership terms. */
    CatalogMaintenance(ScheduledExecutorService invalidationCleanupScheduler) {
        this.invalidationCleanupScheduler = java.util.Objects.requireNonNull(invalidationCleanupScheduler);
    }

    void setMetadataSyncDeps(MetadataSyncService syncService,
                             CatalogRegistry catalog) {
        this.metadataSyncService = syncService;
        this.catalogRegistry = catalog;
    }

    /** Observes LINKED/MANAGED versions on the same leader-gated cadence as sync. */
    void setVersionObserver(TableVersionObserver observer) {
        this.versionObserver = observer;
    }

    void setInvalidationPoller(PostgresInvalidationPoller poller) {
        this.invalidationPoller = poller;
    }

    PostgresInvalidationPoller getInvalidationPoller() {
        return invalidationPoller;
    }

    void start() {
        startMetadataSyncSchedule();
        startInvalidationCleanupSchedule();
    }

    void stop() {
        stopMetadataSyncSchedule();
        stopInvalidationCleanupSchedule();
    }

    private void startMetadataSyncSchedule() {
        if (catalogRegistry == null
                || (metadataSyncService == null && versionObserver == null)) {
            return;
        }
        if (metadataSyncScheduler == null) {
            metadataSyncScheduler = ManagedExecutorFactory.newSingleThreadScheduledExecutor("metadata-sync");
        }
        metadataSyncTask = metadataSyncScheduler.scheduleWithFixedDelay(
                () -> ExecutorDrain.runOwned(metadataSyncScheduler, this::runMetadataSync),
                METADATA_SYNC_INTERVAL_SECONDS, METADATA_SYNC_INTERVAL_SECONDS, TimeUnit.SECONDS);
        LOG.info("event=metadata_sync_started intervalSeconds={}", METADATA_SYNC_INTERVAL_SECONDS);
    }

    private void stopMetadataSyncSchedule() {
        if (metadataSyncTask != null) {
            metadataSyncTask.cancel(false);
            metadataSyncTask = null;
        }
        LOG.info("event=metadata_sync_stopped");
    }

    private void runMetadataSync() {
        try {
            CatalogRegistry.DivergenceReport report =
                    catalogRegistry.reconcileFromStore();
            if (!report.isEmpty()) {
                LOG.info("event=periodic_catalog_reconcile loaded={} removed={} loadFailed={}",
                        report.loaded().size(), report.removed().size(), report.loadFailed().size());
            }
        } catch (Exception e) {
            LOG.warn("event=periodic_catalog_reconcile_failed error={}", e.getMessage(), e);
        }

        if (metadataSyncService != null) {
            try {
                syncCatalogs();
            } catch (RuntimeException failure) {
                LOG.warn("event=metadata_sync_catalog_listing_failed", failure);
            }
        }
        if (versionObserver != null) {
            try {
                observeTableVersions();
            } catch (RuntimeException failure) {
                LOG.warn("event=table_version_observation_failed", failure);
            }
        }
    }

    private void syncCatalogs() {
        for (String catalogName : catalogRegistry.listCatalogNames()) {
            try {
                CatalogHandle handle = catalogRegistry.getCatalog(catalogName);
                handle.federationSource().ifPresent(source ->
                        metadataSyncService.syncWithRetry(handle.getDescriptor(), source));
            } catch (Exception e) {
                LOG.warn("event=metadata_sync_catalog_failed catalog={} error={}",
                        catalogName, e.getMessage(), e);
            }
        }
    }

    private void observeTableVersions() {
        for (TableVersionObservationReport report
                : versionObserver.observeAll()) {
            if (report.versionChanges() > 0 || report.identityChanges() > 0
                    || report.missing() > 0 || report.hasFailures()) {
                LOG.info("event=table_version_observed catalog={} checked={} "
                                + "versionChanges={} identityChanges={} missing={} "
                                + "unsupported={} errors={}",
                        report.catalogName(), report.checked(), report.versionChanges(),
                        report.identityChanges(), report.missing(), report.unsupported(),
                        report.errors().size());
            }
        }
    }

    private void startInvalidationCleanupSchedule() {
        if (invalidationPoller == null) {
            return;
        }
        invalidationCleanupTask = invalidationCleanupScheduler.scheduleWithFixedDelay(
                () -> ExecutorDrain.runOwned(invalidationCleanupScheduler, () -> {
                    try {
                        invalidationPoller.cleanup();
                    } catch (Exception e) {
                        LOG.warn("event=invalidation_cleanup_failed error={}", e.getMessage());
                    }
                }),
                INVALIDATION_CLEANUP_INTERVAL_HOURS, INVALIDATION_CLEANUP_INTERVAL_HOURS, TimeUnit.HOURS);
        LOG.info("event=invalidation_cleanup_started intervalHours={}", INVALIDATION_CLEANUP_INTERVAL_HOURS);
    }

    private void stopInvalidationCleanupSchedule() {
        if (invalidationCleanupTask != null) {
            invalidationCleanupTask.cancel(false);
            invalidationCleanupTask = null;
        }
    }

    @Override
    public void close() {
        ResourceCloser.closeAll(null, this::closeMetadataSync, this::closeInvalidation);
    }

    private void closeMetadataSync() {
        stopMetadataSyncSchedule();
        if (metadataSyncScheduler != null) {
            shutdown(metadataSyncScheduler);
            metadataSyncScheduler = null;
        }
    }

    private void closeInvalidation() {
        stopInvalidationCleanupSchedule();
        if (invalidationCleanupScheduler != null) {
            shutdown(invalidationCleanupScheduler);
            invalidationCleanupScheduler = null;
        }
        // The cleanup task borrows the poller until its executor actually exits.
        if (invalidationPoller != null) {
            invalidationPoller.close();
            invalidationPoller = null;
        }
    }

    private static void shutdown(ScheduledExecutorService scheduler) {
        scheduler.shutdownNow();
        ExecutorDrain.shutdownAndAwait(scheduler, CoordinatorBootstrapConfig.DEFAULT_SHUTDOWN_TIMEOUT_MS);
    }
}
