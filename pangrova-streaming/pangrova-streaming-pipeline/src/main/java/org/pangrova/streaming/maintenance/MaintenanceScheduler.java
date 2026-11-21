package org.pangrova.streaming.maintenance;

import org.pangrova.streaming.enums.TriggerPriority;

import org.pangrova.common.concurrent.ManagedExecutorFactory;
import org.pangrova.common.concurrent.ThreadSafe;
import org.pangrova.connector.api.write.WriteResult;
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

    private final TableMaintenance maintenance;
    private final MaintenanceConfig config;
    private final Semaphore concurrencyLimiter;
    private final PriorityBlockingQueue<MaintenanceTask> taskQueue;
    private final ConcurrentHashMap<String, Instant> lastMaintenanceTime;
    private final ConcurrentHashMap<String, Boolean> inFlightTasks;
    private final ConcurrentHashMap<String, java.util.concurrent.locks.ReentrantLock> tableMaintenanceLocks;
    private final ExecutorService taskExecutor;
    private final ScheduledExecutorService scanScheduler;
    private volatile boolean running = true;
    private volatile ScheduledFuture<?> periodicScanFuture;

    /** Monitor lock protecting cross-map check-then-act operations. */
    private final Object lock = new Object();

    /** Creates a scheduler that drives maintenance tasks through the given SPI. */
    public MaintenanceScheduler(TableMaintenance maintenance, MaintenanceConfig config) {
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
     * <p>Synchronized to make the isInFlight + isIntervalElapsed + enqueue sequence atomic.</p>
     */
    public void onWriteComplete(String tableName, WriteResult result) {
        synchronized (lock) {
            if (isInFlightUnsafe(tableName)) {
                return;
            }
            if (!isIntervalElapsed(tableName, config.compactionInterval())) {
                return;
            }
            enqueueWithBackpressureUnsafe(
                new MaintenanceTask(tableName, TriggerPriority.INTERVAL, Instant.now()));
        }
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
        synchronized (lock) {
            for (String tableName : tableNames) {
                if (isInFlightUnsafe(tableName)) {
                    continue;
                }
                TriggerPriority priority = evaluatePriority(tableName);
                if (priority != null) {
                    enqueueWithBackpressureUnsafe(
                        new MaintenanceTask(tableName, priority, Instant.now()));
                }
            }
        }
    }

    /**
     * Enqueue with backpressure: replace lowest priority if queue is full.
     *
     * <p>Acquires the lock to ensure the size-check + offer/replace is atomic.</p>
     */
    public void enqueueWithBackpressure(MaintenanceTask task) {
        synchronized (lock) {
            enqueueWithBackpressureUnsafe(task);
        }
    }

    /**
     * Process tasks from queue respecting concurrency limits.
     *
     * <p>The semaphore acquire + queue poll + inFlightTasks put sequence is
     * protected by the lock to prevent duplicate task submission.</p>
     */
    public void processTaskQueue() {
        while (running && !taskQueue.isEmpty()) {
            if (!concurrencyLimiter.tryAcquire()) {
                break;
            }
            synchronized (lock) {
                MaintenanceTask task = taskQueue.poll();
                if (task == null) {
                    concurrencyLimiter.release();
                    break;
                }
                inFlightTasks.put(task.tableName(), true);
                taskExecutor.submit(() -> executeTask(task));
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
        scanScheduler.shutdownNow();
        taskExecutor.shutdown();
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

    /** Must be called under {@code lock}. */
    private boolean isInFlightUnsafe(String tableName) {
        return inFlightTasks.getOrDefault(tableName, false);
    }

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

    /** Must be called under {@code lock}. */
    private void enqueueWithBackpressureUnsafe(MaintenanceTask task) {
        if (taskQueue.size() < config.taskQueueCapacity()) {
            taskQueue.offer(task);
            return;
        }
        replaceLowestPriority(task);
    }

    /** Must be called under {@code lock}. */
    private void replaceLowestPriority(MaintenanceTask newTask) {
        MaintenanceTask lowest = taskQueue.stream()
            .max(Comparator.comparingInt(t -> t.priority().ordinal()))
            .orElse(null);
        if (lowest != null && newTask.priority().ordinal() < lowest.priority().ordinal()) {
            taskQueue.remove(lowest);
            taskQueue.offer(newTask);
        }
    }

    private void executeTask(MaintenanceTask task) {
        // F-STREAM-05: Acquire per-table maintenance lock to prevent concurrent
        // CDC writes from racing with compaction (merge-tree corruption risk).
        java.util.concurrent.locks.ReentrantLock tableLock =
                tableMaintenanceLocks.computeIfAbsent(task.tableName(),
                        k -> new java.util.concurrent.locks.ReentrantLock());
        tableLock.lock();
        // The future returned by the SPI must be cancelled on timeout; a derived
        // future does not propagate cancellation upstream, so track it separately.
        CompletableFuture<?> issued = null;
        try {
            // A write-triggered task is a compaction of the data just written. Only
            // the periodic scan visits the full retention policy, so expiry and
            // orphan cleanup cannot be triggered by a single write on a node that
            // does not hold maintenance leadership.
            CompletableFuture<Void> awaited;
            if (task.priority() == TriggerPriority.SCHEDULED_SCAN) {
                awaited = maintenance.runScheduledMaintenanceAsync(task.tableName());
                issued = awaited;
            } else {
                CompletableFuture<CompactionResult> compaction =
                        maintenance.compactAsync(task.tableName());
                issued = compaction;
                awaited = compaction.thenApply(ignored -> null);
            }
            awaited
                .orTimeout(config.taskTimeout().toMillis(), TimeUnit.MILLISECONDS)
                .join();
            synchronized (lock) {
                lastMaintenanceTime.put(task.tableName(), Instant.now());
            }
            LOG.info("event=maintenance_cycle_completed table={} priority={}",
                    task.tableName(), task.priority());
        } catch (Exception e) {
            // On timeout (or any failure), cancel the future issued by the SPI so the
            // underlying operation is interrupted rather than left running indefinitely.
            if (issued != null) {
                issued.cancel(true);
            }
            LOG.warn("event=maintenance_failed table={} error={}", task.tableName(), e.getMessage(), e);
        } finally {
            tableLock.unlock();
            synchronized (lock) {
                inFlightTasks.remove(task.tableName());
            }
            concurrencyLimiter.release();
        }
    }

    /**
     * Returns the per-table maintenance lock. CDC writers should use tryLock()
     * to skip maintenance trigger if compaction is in progress.
     */
    public java.util.concurrent.locks.ReentrantLock getTableMaintenanceLock(String tableName) {
        return tableMaintenanceLocks.computeIfAbsent(tableName,
                k -> new java.util.concurrent.locks.ReentrantLock());
    }
}
