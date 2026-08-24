package org.pangrova.coordinator.worker;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.pangrova.common.util.Preconditions.checkNotNull;

import org.pangrova.common.concurrent.ThreadSafe;

/**
 * Periodic health evaluator for all registered Workers.
 * Detects DEAD workers, invokes a callback per dead workerId,
 * then removes them from the registry.
 *
 * <p>Concurrency strategy: volatile for scheduled task reference;
 * delegates to thread-safe WorkerManager for state transitions.</p>
 */
@ThreadSafe
public class WorkerHealthScheduler {

    private static final Logger LOG = LoggerFactory.getLogger(WorkerHealthScheduler.class);
    private static final long DEFAULT_INTERVAL_MS = 10_000;
    /** C-WK-09: Max time a worker can stay in DECOMMISSIONING before forced removal. */
    private static final long DECOMMISSION_TIMEOUT_MS = 300_000; // 5 minutes

    private final WorkerManager workerManager;
    private final Consumer<String> deadWorkerCallback;
    private final ScheduledExecutorService scheduler;
    private final long intervalMs;
    private volatile ScheduledFuture<?> scheduledTask;

    /**
     * Creates a scheduler with default health check interval (10s).
     */
    public WorkerHealthScheduler(WorkerManager workerManager,
                                 Consumer<String> deadWorkerCallback,
                                 ScheduledExecutorService scheduler) {
        this(workerManager, deadWorkerCallback, scheduler, DEFAULT_INTERVAL_MS);
    }

    /**
     * Creates a scheduler with configurable health check interval.
     *
     * @param workerManager      worker registry and health evaluator
     * @param deadWorkerCallback callback invoked per dead workerId
     * @param scheduler          shared scheduled executor
     * @param intervalMs         health check interval in milliseconds
     */
    public WorkerHealthScheduler(WorkerManager workerManager,
                                 Consumer<String> deadWorkerCallback,
                                 ScheduledExecutorService scheduler,
                                 long intervalMs) {
        this.workerManager = checkNotNull(workerManager, "workerManager must not be null");
        this.deadWorkerCallback = checkNotNull(deadWorkerCallback, "deadWorkerCallback must not be null");
        this.scheduler = checkNotNull(scheduler, "scheduler must not be null");
        if (intervalMs <= 0) {
            throw new IllegalArgumentException("intervalMs must be > 0");
        }
        this.intervalMs = intervalMs;
    }

    public void start() {
        scheduledTask = scheduler.scheduleAtFixedRate(
                this::evaluateAll,
                intervalMs,
                intervalMs,
                TimeUnit.MILLISECONDS);
        LOG.info("event=health_scheduler_started intervalMs={}", intervalMs);
    }

    public void stop() {
        ScheduledFuture<?> task = scheduledTask;
        if (task != null) {
            task.cancel(false);
            LOG.info("event=health_scheduler_stopped");
        }
    }

    void evaluateAll() {
        try {
            Instant now = Instant.now();
            List<WorkerInfo> workers = workerManager.getAllWorkers();
            for (WorkerInfo worker : workers) {
                if (worker.healthState() == HealthState.DECOMMISSIONING) {
                    evaluateDecommission(worker, now);
                } else {
                    evaluateSingle(worker.workerId(), now);
                }
            }
            workerManager.expireStaleFromRegistry(Duration.ofMillis(workerManager.deadThresholdMs()));
        } catch (Exception e) {
            LOG.error("event=health_check_failed error={}", e.getMessage(), e);
        }
    }

    /**
     * C-WK-09: Checks if a decommissioning worker has either drained all tasks
     * or exceeded the decommission timeout. In either case, removes the worker.
     *
     * <p>Timeout is measured from the last heartbeat, not from decommission start.
     * A decommissioning worker that is still sending heartbeats is considered
     * healthy and draining. The timeout only fires when the worker has ALSO
     * stopped heartbeating (likely crashed during drain), combining the
     * decommission state with heartbeat staleness as a compound signal.</p>
     */
    private void evaluateDecommission(WorkerInfo worker, Instant now) {
        if (workerManager.isDecommissionComplete(worker.workerId())) {
            LOG.info("event=decommission_complete workerId={}", worker.workerId());
            workerManager.removeWorker(worker.workerId());
            return;
        }
        long heartbeatAge = Duration.between(worker.lastHeartbeat(), now).toMillis();
        // Only force-remove if the worker has stopped heartbeating AND exceeded
        // the decommission timeout. A worker still sending heartbeats is alive
        // and draining — give it more time.
        if (heartbeatAge > workerManager.deadThresholdMs()
                && heartbeatAge > DECOMMISSION_TIMEOUT_MS) {
            LOG.warn("event=decommission_timeout workerId={} heartbeatAgeMs={} runningTasks={}",
                    worker.workerId(), heartbeatAge, worker.runningTaskCount());
            try {
                deadWorkerCallback.accept(worker.workerId());
            } catch (Exception e) {
                LOG.error("event=decommission_timeout_callback_failed workerId={}", worker.workerId(), e);
            }
            workerManager.removeWorker(worker.workerId());
        }
    }

    private void evaluateSingle(String workerId, Instant now) {
        HealthState state = workerManager.evaluateHealth(workerId, now);
        if (state == HealthState.SUSPECT) {
            logSuspect(workerId);
        } else if (state == HealthState.DEAD) {
            logDead(workerId);
            // C-WK-08: Invoke callback first (may trigger task rescheduling),
            // then remove. The callback is synchronous so rescheduling completes
            // before the worker is removed from the active set.
            try {
                deadWorkerCallback.accept(workerId);
            } catch (Exception e) {
                LOG.error("event=dead_worker_callback_failed workerId={} error={}",
                        workerId, e.getMessage(), e);
            }
            workerManager.removeWorker(workerId);
        }
    }

    private void logSuspect(String workerId) {
        WorkerInfo info = workerManager.getWorker(workerId);
        if (info != null) {
            LOG.warn("event=worker_suspect workerId={} lastHeartbeat={}", workerId, info.lastHeartbeat());
        }
    }

    private void logDead(String workerId) {
        LOG.error("event=worker_dead workerId={} action=removing_from_registry", workerId);
    }
}
