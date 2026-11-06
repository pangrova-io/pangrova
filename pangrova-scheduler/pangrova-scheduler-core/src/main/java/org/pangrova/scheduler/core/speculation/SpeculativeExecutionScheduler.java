package org.pangrova.scheduler.core.speculation;

import java.io.Closeable;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import org.pangrova.common.concurrent.ThreadSafe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.pangrova.common.util.Preconditions.checkNotNull;

/**
 * Periodically evaluates all running queries for straggler tasks and launches
 * speculative copies. Decouples speculation from stage-finish events, enabling
 * speculation for single-stage queries.
 *
 * <p>Lifecycle: call {@link #start()} after construction, {@link #close()} on shutdown.
 * The scheduler uses a single-thread {@link ScheduledExecutorService} with
 * {@code setRemoveOnCancelPolicy(true)} per pangrova-rules §6.</p>
 */
@ThreadSafe
public final class SpeculativeExecutionScheduler implements Closeable {
    private static final long EXECUTOR_SHUTDOWN_TIMEOUT_MS = 5_000L;

    private static final Logger LOG = LoggerFactory.getLogger(SpeculativeExecutionScheduler.class);

    private final SpeculativeStageHandler stageHandler;
    private final SpeculativeQueryInventory queryInventory;
    private final SpeculativeTaskRegistry taskRegistry;
    private final long intervalMs;
    private final ScheduledExecutorService executor;
    private volatile ScheduledFuture<?> scheduledTask;

    /** Creates a scheduler that polls for stragglers at the given interval. */
    public SpeculativeExecutionScheduler(SpeculativeStageHandler stageHandler,
                                         SpeculativeQueryInventory queryInventory,
                                         SpeculativeTaskRegistry taskRegistry,
                                         long intervalMs) {
        this.stageHandler = checkNotNull(stageHandler, "stageHandler must not be null");
        this.queryInventory = checkNotNull(queryInventory, "queryInventory must not be null");
        this.taskRegistry = checkNotNull(taskRegistry, "taskRegistry must not be null");
        if (intervalMs <= 0) {
            throw new IllegalArgumentException("intervalMs must be positive, got " + intervalMs);
        }
        this.intervalMs = intervalMs;
        var ses = new ScheduledThreadPoolExecutor(1, r -> {
            Thread t = new Thread(r, "speculation-scheduler");
            t.setDaemon(true);
            return t;
        });
        ses.setRemoveOnCancelPolicy(true);
        this.executor = ses;
    }

    /**
     * Starts the periodic straggler evaluation loop.
     */
    public void start() {
        scheduledTask = executor.scheduleAtFixedRate(
                () -> org.pangrova.common.concurrent.ExecutorDrain.runOwned(executor, this::evaluateAllQueries),
                intervalMs, intervalMs, TimeUnit.MILLISECONDS);
        LOG.info("event=speculation_scheduler_started intervalMs={}", intervalMs);
    }

    /**
     * Shuts down the scheduler gracefully. Idempotent.
     */
    @Override
    public void close() {
        ScheduledFuture<?> task = scheduledTask;
        if (task != null) {
            task.cancel(false);
        }
        org.pangrova.common.concurrent.ExecutorDrain.shutdownAndAwait(executor, EXECUTOR_SHUTDOWN_TIMEOUT_MS);
        LOG.info("event=speculation_scheduler_stopped");
    }

    private void evaluateAllQueries() {
        try {
            Set<String> activeQueryIds = queryInventory.activeQueryIds();
            for (String queryId : activeQueryIds) {
                evaluateQuery(queryId);
            }
        } catch (Exception e) {
            LOG.warn("event=speculation_scheduler_error error={}", e.getMessage(), e);
        }
    }

    private void evaluateQuery(String queryId) {
        Set<String> runningStageIds = taskRegistry.getRunningStageIds(queryId);
        for (String stageId : runningStageIds) {
            stageHandler.evaluateStragglers(queryId, stageId);
        }
    }
}
