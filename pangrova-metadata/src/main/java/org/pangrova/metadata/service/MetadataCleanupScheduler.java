package org.pangrova.metadata.service;

import org.pangrova.common.concurrent.ManagedExecutorFactory;
import org.pangrova.common.concurrent.ThreadSafe;
import org.pangrova.metadata.store.jdbc.cache.JdbcInvalidationPoller;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.util.Objects;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Schedules periodic invalidation-log pruning and stale-cache eviction.
 *
 * <p>Uses a single-thread {@link ScheduledExecutorService} created via
 * {@link ManagedExecutorFactory} which sets {@code removeOnCancelPolicy(true)}
 * per pangrova-rules §6. Graceful shutdown cancels scheduled tasks and awaits
 * termination.</p>
 *
 * <p>Lifecycle: construct → {@link #start()} → {@link #close()}.
 * Calling {@link #close()} multiple times is safe (idempotent).</p>
 */
@ThreadSafe
public final class MetadataCleanupScheduler implements Closeable {

    private static final Logger LOG = LoggerFactory.getLogger(MetadataCleanupScheduler.class);
    private static final long SHUTDOWN_TIMEOUT_SECONDS = 5;

    private final JdbcInvalidationPoller invalidationPoller;
    private final Runnable cacheMaintenance;
    private final MetadataCleanupConfig config;
    private final ScheduledExecutorService scheduler;
    private final AtomicBoolean closed = new AtomicBoolean(false);

    private final AtomicReference<ScheduledFuture<?>> scheduledTask = new AtomicReference<>();

    /**
     * Creates a cleanup scheduler with the given dependencies.
     *
     * @param invalidationPoller poller for invalidation log cleanup
     * @param cacheMaintenance   runnable that triggers cache eviction maintenance
     * @param config             cleanup configuration (interval, enabled flags)
     */
    public MetadataCleanupScheduler(JdbcInvalidationPoller invalidationPoller,
                                    Runnable cacheMaintenance,
                                    MetadataCleanupConfig config) {
        this.invalidationPoller = Objects.requireNonNull(invalidationPoller,
                "invalidationPoller must not be null");
        this.cacheMaintenance = Objects.requireNonNull(cacheMaintenance,
                "cacheMaintenance must not be null");
        this.config = Objects.requireNonNull(config, "config must not be null");
        this.scheduler = ManagedExecutorFactory.newSingleThreadScheduledExecutor(
                "metadata-cleanup");
    }

    /**
     * Package-private constructor for testing with a custom scheduler.
     */
    MetadataCleanupScheduler(JdbcInvalidationPoller invalidationPoller,
                             Runnable cacheMaintenance,
                             MetadataCleanupConfig config,
                             ScheduledExecutorService scheduler) {
        this.invalidationPoller = Objects.requireNonNull(invalidationPoller,
                "invalidationPoller must not be null");
        this.cacheMaintenance = Objects.requireNonNull(cacheMaintenance,
                "cacheMaintenance must not be null");
        this.config = Objects.requireNonNull(config, "config must not be null");
        this.scheduler = Objects.requireNonNull(scheduler, "scheduler must not be null");
    }

    /**
     * Starts the periodic cleanup task. Idempotent: subsequent calls are no-ops.
     */
    public void start() {
        if (closed.get()) {
            LOG.warn("event=cleanup_start_rejected reason=already_closed");
            return;
        }
        long intervalMinutes = config.intervalMinutes();
        ScheduledFuture<?> newTask = scheduler.scheduleAtFixedRate(
                this::runCleanup, intervalMinutes, intervalMinutes, TimeUnit.MINUTES);
        if (!scheduledTask.compareAndSet(null, newTask)) {
            newTask.cancel(false);
            LOG.debug("event=cleanup_start_skipped reason=already_running");
            return;
        }
        LOG.info("event=metadata_cleanup_started intervalMinutes={}", intervalMinutes);
    }

    /**
     * Gracefully shuts down the scheduler: cancels scheduled tasks and awaits
     * termination. Idempotent: calling multiple times is safe.
     */
    @Override
    public void close() {
        if (!closed.compareAndSet(false, true)) {
            return;
        }
        ScheduledFuture<?> task = scheduledTask.getAndSet(null);
        if (task != null) {
            task.cancel(false);
        }
        scheduler.shutdown();
        try {
            if (!scheduler.awaitTermination(SHUTDOWN_TIMEOUT_SECONDS, TimeUnit.SECONDS)) {
                scheduler.shutdownNow();
                LOG.warn("event=metadata_cleanup_forced_shutdown");
            }
        } catch (InterruptedException e) {
            scheduler.shutdownNow();
            Thread.currentThread().interrupt();
            LOG.warn("event=metadata_cleanup_shutdown_interrupted");
        }
        LOG.info("event=metadata_cleanup_stopped");
    }

    /**
     * Executes all enabled cleanup tasks in sequence. Exceptions from one task
     * do not prevent subsequent tasks from running.
     */
    void runCleanup() {
        LOG.debug("event=metadata_cleanup_cycle_start");
        int totalCleaned = 0;

        if (config.invalidationCleanupEnabled()) {
            totalCleaned += cleanInvalidationLog();
        }
        if (config.cacheCleanupEnabled()) {
            evictStaleCache();
        }

        if (totalCleaned > 0) {
            LOG.info("event=metadata_cleanup_cycle_complete totalCleaned={}", totalCleaned);
        } else {
            LOG.debug("event=metadata_cleanup_cycle_complete totalCleaned=0");
        }
    }

    private int cleanInvalidationLog() {
        try {
            int deleted = invalidationPoller.cleanup();
            if (deleted > 0) {
                LOG.info("event=invalidation_log_cleanup deleted={}", deleted);
            }
            return deleted;
        } catch (RuntimeException e) {
            LOG.warn("event=invalidation_log_cleanup_failed error={}", e.getMessage(), e);
            return 0;
        }
    }

    private void evictStaleCache() {
        try {
            cacheMaintenance.run();
            LOG.debug("event=cache_maintenance_complete");
        } catch (RuntimeException e) {
            LOG.warn("event=cache_maintenance_failed error={}", e.getMessage(), e);
        }
    }
}
