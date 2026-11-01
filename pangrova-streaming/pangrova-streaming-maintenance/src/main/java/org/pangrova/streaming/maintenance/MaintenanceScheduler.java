package org.pangrova.streaming.maintenance;

import org.pangrova.common.concurrent.ManagedExecutorFactory;
import org.pangrova.common.concurrent.ThreadSafe;
import org.pangrova.spi.connector.data.write.model.WriteResult;
import org.pangrova.streaming.maintenance.policy.TriggerPriority;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.Collection;
import java.util.Comparator;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

/**
 * Unified priority trigger model with global concurrency control.
 * Manages compaction scheduling via priority queue and semaphore-based throttling.
 *
 * <p>Concurrency strategy: a single monitor lock ({@code lock}) guards all
 * check-then-act sequences that span multiple ConcurrentHashMap instances
 * ({@code inFlightTasks}, {@code lastMaintenanceTime}) and the task queue.
 * Individual map reads outside the lock are safe because ConcurrentHashMap
 * provides volatile-read semantics, but any compound operation (read-then-write
 * or read-from-A-then-write-to-B) must hold the lock.</p>
 */
@ThreadSafe
public class MaintenanceScheduler implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(MaintenanceScheduler.class);

    private final TableMaintenanceSpi maintenance;
    private final MaintenanceConfig config;
    private final Semaphore concurrencyLimiter;
    private final PriorityBlockingQueue<MaintenanceTask> taskQueue;
    private final ConcurrentHashMap<String, Instant> lastMaintenanceTime;
    private final ConcurrentHashMap<String, Boolean> inFlightTasks;
    private final java.util.Set<String> queuedTables = ConcurrentHashMap.newKeySet();
    /** SPI futures issued to the maintenance backend; cancelled on close. */
    private final java.util.Set<CompletableFuture<?>> issuedSpiFutures = ConcurrentHashMap.newKeySet();
    private final ConcurrentHashMap<String, TableMaintenanceLock> tableMaintenanceLocks;
    private final ExecutorService taskExecutor;
    private final ScheduledExecutorService scanScheduler;
    private volatile boolean running = true;
    private volatile ScheduledFuture<?> periodicScanFuture;

    /** Monitor lock protecting cross-map check-then-act operations. */
    private final Object lock = new Object();

    /** Creates a scheduler that drives maintenance tasks through the given SPI. */
    public MaintenanceScheduler(TableMaintenanceSpi maintenance, MaintenanceConfig config) {
        this.maintenance = maintenance;
        this.config = config;
        this.concurrencyLimiter = new Semaphore(config.maxConcurrentTasks());
        this.taskQueue = new PriorityBlockingQueue<>(config.taskQueueCapacity());
        this.lastMaintenanceTime = new ConcurrentHashMap<>();
        this.inFlightTasks = new ConcurrentHashMap<>();
        this.tableMaintenanceLocks = new ConcurrentHashMap<>();
        this.taskExecutor = ManagedExecutorFactory.newFixedThreadPool(
            "maintenance-scheduler", config.threads() + 1);
        this.scanScheduler = ManagedExecutorFactory.newSingleThreadScheduledExecutor(
            "maintenance-scan");
    }

    /**
     * Called after each write completes. Enqueues INTERVAL task if cooldown elapsed.
     *
     * <p>The isInFlight/isIntervalElapsed checks are lock-free pre-filters; the
     * authoritative in-flight re-check and enqueue are atomic under {@code lock}
     * inside {@link #reserveAndEnqueue}.</p>
     */
    public void onWriteComplete(String tableName, WriteResult result) {
        if (isInFlight(tableName)) {
            return;
        }
        if (!isIntervalElapsed(tableName, config.compactionInterval())) {
            return;
        }
        reserveAndEnqueue(new MaintenanceTask(tableName, TriggerPriority.INTERVAL, Instant.now()));
        // Dispatch immediately so write-triggered tasks don't wait for the next periodic scan.
        processTaskQueue();
    }

    /**
     * Periodic scan: evaluate priority for each table and enqueue if needed.
     *
     * <p>Synchronized to make the isInFlight + evaluatePriority + enqueue sequence atomic
     * for each table within a single scan pass.</p>
     */
    public void scanAllTablesWithPriority(Collection<String> tableNames) {
        for (String tableName : tableNames) {
            if (isInFlight(tableName)) {
                continue;
            }
            // SPI status reads may perform I/O: never hold the scheduler lock here.
            TriggerPriority priority = evaluatePriority(tableName);
            if (priority != null) {
                reserveAndEnqueue(new MaintenanceTask(tableName, priority, Instant.now()));
            }
        }
    }

    /**
     * Enqueue with backpressure: replace lowest priority if queue is full.
     */
    public void enqueueWithBackpressure(MaintenanceTask task) {
        reserveAndEnqueue(task);
    }

    /**
     * Process tasks from queue respecting concurrency limits.
     */
    public void processTaskQueue() {
        while (running) {
            if (!concurrencyLimiter.tryAcquire()) {
                return;
            }
            MaintenanceTask task;
            synchronized (lock) {
                task = taskQueue.poll();
                if (task != null) {
                    queuedTables.remove(task.tableName());
                    inFlightTasks.put(task.tableName(), Boolean.TRUE);
                }
            }
            if (task == null) {
                concurrencyLimiter.release();
                return;
            }
            try {
                taskExecutor.submit(() -> executeTask(task));
            } catch (RuntimeException rejected) {
                releaseTask(task);
                if (!running) {
                    return;
                }
                synchronized (lock) {
                    if (queuedTables.add(task.tableName())) {
                        taskQueue.offer(task);
                    }
                }
                LOG.warn("event=maintenance_dispatch_rejected table={} error={}",
                        task.tableName(), rejected.getMessage());
                return;
            }
        }
    }

    private void reserveAndEnqueue(MaintenanceTask task) {
        synchronized (lock) {
            if (inFlightTasks.containsKey(task.tableName())
                    || !queuedTables.add(task.tableName())) {
                return;
            }
            if (!enqueueWithBackpressureUnsafe(task)) {
                queuedTables.remove(task.tableName());
            }
        }
    }

    /**
     * Starts a periodic scan thread that discovers tables needing maintenance
     * and submits maintenance tasks. The scan runs at the configured compaction
     * scan interval.
     *
     * @param tableSource provides the set of managed tables to scan
     */
    public void startPeriodicScan(MaintenanceTableSource tableSource) {
        if (!running) {
            return;
        }
        long intervalMs = config.compactionScanInterval().toMillis();
        stopPeriodicScan();
        periodicScanFuture = scanScheduler.scheduleAtFixedRate(() -> {
            try {
                Collection<String> tables = tableSource.listManagedTables();
                scanAllTablesWithPriority(tables);
                processTaskQueue();
            } catch (Exception e) {
                LOG.warn("event=periodic_scan_failed error={}", e.getMessage(), e);
            }
        }, intervalMs, intervalMs, TimeUnit.MILLISECONDS);
        LOG.info("event=periodic_scan_started interval={}", config.compactionScanInterval());
    }

    /**
     * Stops the periodic scan thread if running.
     */
    public void stopPeriodicScan() {
        ScheduledFuture<?> future = periodicScanFuture;
        if (future != null) {
            future.cancel(false);
            periodicScanFuture = null;
        }
    }

    /** True while the leader-gated periodic scan is scheduled. */
    public boolean isPeriodicScanRunning() {
        ScheduledFuture<?> future = periodicScanFuture;
        return future != null && !future.isCancelled() && !future.isDone();
    }

    @Override
    public void close() {
        running = false;
        stopPeriodicScan();
        // Cancel issued SPI futures first so in-flight maintenance cannot keep
        // writing to storage after the scheduler is closed. Cancellation is
        // best-effort; a backend that ignores the interrupt is logged on
        // completion, not waited for.
        for (CompletableFuture<?> future : issuedSpiFutures) {
            future.cancel(true);
        }
        issuedSpiFutures.clear();
        scanScheduler.shutdownNow();
        taskExecutor.shutdownNow();
        synchronized (lock) {
            taskQueue.clear();
            inFlightTasks.clear();
            queuedTables.clear();
        }
    }

    /**
     * Thread-safe check whether a table has an in-flight maintenance task.
     * Safe for standalone reads (ConcurrentHashMap provides volatile semantics).
     */
    public boolean isInFlight(String tableName) {
        return inFlightTasks.getOrDefault(tableName, false);
    }

    public int queueSize() {
        return taskQueue.size();
    }

    // --- internal methods (caller must hold lock where noted) ---

    private TriggerPriority evaluatePriority(String tableName) {
        MaintenanceStatus status = maintenance.getMaintenanceStatus(tableName);
        if (status.equalityDeleteFiles() >= config.maxEqualityDeleteFiles()) {
            return TriggerPriority.THRESHOLD;
        }
        if (hasSmallFilesReady(tableName, status)) {
            return TriggerPriority.SCHEDULED_SCAN;
        }
        if (maintenance.isScheduledMaintenanceDue(tableName)) {
            return TriggerPriority.SCHEDULED_SCAN;
        }
        return null;
    }

    private boolean hasSmallFilesReady(String tableName, MaintenanceStatus status) {
        return status.smallFilesCount() >= config.minFilesToCompact()
            && isIntervalElapsed(tableName, config.compactionScanInterval());
    }

    private boolean isIntervalElapsed(String tableName, Duration interval) {
        Instant last = lastMaintenanceTime.get(tableName);
        return last == null || Instant.now().isAfter(last.plus(interval));
    }

    /** Must be called under {@code lock}; returns whether the task was enqueued. */
    private boolean enqueueWithBackpressureUnsafe(MaintenanceTask task) {
        if (taskQueue.size() < config.taskQueueCapacity()) {
            taskQueue.offer(task);
            return true;
        }
        return replaceLowestPriority(task);
    }

    /** Must be called under {@code lock}; returns whether the new task replaced another. */
    private boolean replaceLowestPriority(MaintenanceTask newTask) {
        MaintenanceTask lowest = taskQueue.stream()
            .max(Comparator.comparingInt(t -> t.priority().ordinal()))
            .orElse(null);
        if (lowest != null && newTask.priority().ordinal() < lowest.priority().ordinal()) {
            taskQueue.remove(lowest);
            queuedTables.remove(lowest.tableName());
            taskQueue.offer(newTask);
            return true;
        }
        return false;
    }

    private void executeTask(MaintenanceTask task) {
        // F-STREAM-05: per-table lock excludes concurrent CDC writes from compaction.
        TableMaintenanceLock tableLock =
                tableMaintenanceLocks.computeIfAbsent(task.tableName(), k -> new TableMaintenanceLock());
        boolean locked;
        try {
            locked = tableLock.tryLock(5, TimeUnit.SECONDS);
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            releaseTask(task);
            return;
        }
        if (!locked) {
            LOG.warn("event=maintenance_lock_busy table={}", task.tableName());
            releaseTask(task);
            return;
        }

        CompletableFuture<Void> awaited;
        CompletableFuture<?> issued;
        try {
            if (task.priority() == TriggerPriority.SCHEDULED_SCAN) {
                awaited = maintenance.runScheduledMaintenanceAsync(task.tableName());
                issued = awaited;
            } else {
                CompletableFuture<CompactionResult> compaction =
                        maintenance.compactAsync(task.tableName());
                issued = compaction;
                awaited = compaction.thenApply(ignored -> null);
            }
        } catch (RuntimeException synchronousFailure) {
            tableLock.unlock();
            releaseTask(task);
            LOG.warn("event=maintenance_failed table={} error={}",
                    task.tableName(), synchronousFailure.getMessage(), synchronousFailure);
            return;
        }

        CompletableFuture<?> issuedFuture = issued;
        issuedSpiFutures.add(issuedFuture);
        awaited.orTimeout(config.taskTimeout().toMillis(), TimeUnit.MILLISECONDS)
                .whenComplete((ignored, error) -> {
                    issuedSpiFutures.remove(issuedFuture);
                    if (error != null) {
                        // A derived future does not propagate cancellation upstream.
                        issuedFuture.cancel(true);
                        LOG.warn("event=maintenance_failed table={} error={}",
                                task.tableName(), error.getMessage(), error);
                    } else {
                        synchronized (lock) {
                            lastMaintenanceTime.put(task.tableName(), Instant.now());
                        }
                        LOG.info("event=maintenance_cycle_completed table={} priority={}",
                                task.tableName(), task.priority());
                    }
                    tableLock.unlock();
                    releaseTask(task);
                });
    }

    private void releaseTask(MaintenanceTask task) {
        synchronized (lock) {
            inFlightTasks.remove(task.tableName());
        }
        concurrencyLimiter.release();
    }

    /**
     * Returns the per-table maintenance lock. CDC writers should use tryLock()
     * to skip maintenance trigger if compaction is in progress.
     */
    public TableMaintenanceLock getTableMaintenanceLock(String tableName) {
        return tableMaintenanceLocks.computeIfAbsent(tableName, k -> new TableMaintenanceLock());
    }
}
