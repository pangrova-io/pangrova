package org.pangrova.coordinator.dynamictable.schedule;

import org.pangrova.common.concurrent.ManagedExecutorFactory;
import org.pangrova.common.concurrent.ThreadSafe;
import org.pangrova.coordinator.dynamictable.DynamicTableConfig;
import org.pangrova.coordinator.dynamictable.refresh.DagProvider;
import org.pangrova.coordinator.dynamictable.refresh.RefreshRegistrar;
import org.pangrova.coordinator.dynamictable.refresh.pipeline.RefreshResult;
import org.pangrova.metadata.dynamictable.DynamicTableState;
import org.pangrova.metadata.dynamictable.RefreshStatus;
import org.pangrova.metadata.dynamictable.TriggerType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Schedules periodic and on-demand refreshes for Dynamic Tables.
 *
 * <p>Concurrency strategy: all maps use {@link ConcurrentHashMap},
 * counters use {@link AtomicInteger}/{@link AtomicBoolean}.
 * The single-thread scheduler serializes task submission.</p>
 */
@ThreadSafe
public final class RefreshScheduler implements AutoCloseable, RefreshRegistrar {

    private static final Logger LOG = LoggerFactory.getLogger(RefreshScheduler.class);
    private static final double LAG_WARNING_RATIO = 0.8;
    private static final int MAX_BACKOFF_EXPONENT = 20;

    private final RefreshSchedulerContext deps;
    private final DynamicTableConfig config;
    private final ConcurrentHashMap<String, TenantScheduleQueue> tenantQueues = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, ScheduledFuture<?>> scheduledTasks = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, DynamicTableState> registeredStates = new ConcurrentHashMap<>();
    private final ScheduledExecutorService scheduler;
    private final AtomicInteger globalActiveCount = new AtomicInteger(0);
    private final AtomicBoolean closed = new AtomicBoolean(false);

    public RefreshScheduler(RefreshSchedulerContext deps) {
        this.deps = Objects.requireNonNull(deps, "deps must not be null");
        this.config = deps.config();
        this.scheduler = ManagedExecutorFactory.newSingleThreadScheduledExecutor("dt-refresh-scheduler");
    }

    // package-private constructor for testing with custom executor
    RefreshScheduler(RefreshSchedulerContext deps, ScheduledExecutorService scheduler) {
        this.deps = Objects.requireNonNull(deps, "deps must not be null");
        this.config = deps.config();
        this.scheduler = Objects.requireNonNull(scheduler, "scheduler must not be null");
    }

    @Override
    public void register(DynamicTableState state) {
        Objects.requireNonNull(state, "state must not be null");
        TenantScheduleQueue queue = getOrCreateQueue(state.tenantId());
        if (!queue.canRegister()) {
            LOG.warn("event=dt_register_rejected dtName={} tenantId={} reason=quota_exceeded",
                    state.dtName(), state.tenantId());
            return;
        }
        queue.register();
        registeredStates.put(state.dtName(), state);
        schedulePeriodicRefresh(state);
        logQueueMetrics(state.tenantId(), queue);
    }

    /** Unregisters the given Dynamic Table and cancels its scheduled refresh. */
    public void unregister(String dtName) {
        Objects.requireNonNull(dtName, "dtName must not be null");
        cancelFuture(dtName);
        DynamicTableState removed = registeredStates.remove(dtName);
        if (removed != null) {
            TenantScheduleQueue queue = tenantQueues.get(removed.tenantId());
            if (queue != null) {
                queue.unregister();
            }
        }
    }

    /** Updates the target lag of a registered Dynamic Table, rescheduling its refresh. */
    public void updateTargetLag(String dtName, Duration newLag) {
        Objects.requireNonNull(dtName, "dtName must not be null");
        Objects.requireNonNull(newLag, "newLag must not be null");
        DynamicTableState existing = registeredStates.get(dtName);
        if (existing == null) {
            return;
        }
        cancelFuture(dtName);
        DynamicTableState updated = withTargetLag(existing, newLag);
        registeredStates.put(dtName, updated);
        schedulePeriodicRefresh(updated);
    }

    /** Triggers an immediate manual refresh for the given registered Dynamic Table. */
    public void triggerManualRefresh(String dtName) {
        Objects.requireNonNull(dtName, "dtName must not be null");
        DynamicTableState state = registeredStates.get(dtName);
        if (state == null) {
            LOG.warn("event=dt_manual_trigger_ignored dtName={} reason=not_registered", dtName);
            return;
        }
        scheduler.submit(() -> executeRefreshSafely(state.dtName(), TriggerType.MANUAL));
    }

    /** Cascades refresh to registered direct downstreams of the given upstream table. */
    public void onUpstreamRefreshComplete(String upstreamDtName, DagProvider dagProvider) {
        Objects.requireNonNull(upstreamDtName, "upstreamDtName must not be null");
        Objects.requireNonNull(dagProvider, "dagProvider must not be null");
        Set<String> downstreams = dagProvider.getDirectDownstreams(upstreamDtName);
        for (String downstream : downstreams) {
            if (registeredStates.containsKey(downstream)) {
                scheduler.submit(() -> executeRefreshSafely(downstream, TriggerType.CASCADE));
            }
        }
    }

    /** Stops all scheduled tasks and shuts the scheduler down (idempotent). */
    public void stopAll() {
        if (!closed.compareAndSet(false, true)) {
            return;
        }
        scheduledTasks.values().forEach(f -> f.cancel(false));
        scheduledTasks.clear();
        registeredStates.clear();
        tenantQueues.clear();
        globalActiveCount.set(0);
        scheduler.shutdownNow();
    }

    @Override
    public void close() {
        stopAll();
    }

    public int globalActiveCount() { return globalActiveCount.get(); }
    public int registeredCount() { return registeredStates.size(); }
    public TenantScheduleQueue getTenantQueue(String tenantId) { return tenantQueues.get(tenantId); }
    public boolean isRegistered(String dtName) { return registeredStates.containsKey(dtName); }

    private void schedulePeriodicRefresh(DynamicTableState state) {
        long intervalMs = computeIntervalMs(state.targetLag());
        ScheduledFuture<?> future = scheduler.scheduleAtFixedRate(
                () -> executeRefreshSafely(state.dtName(), TriggerType.SCHEDULED),
                intervalMs, intervalMs, TimeUnit.MILLISECONDS);
        scheduledTasks.put(state.dtName(), future);
    }

    private void executeRefreshSafely(String dtName, TriggerType triggerType) {
        try {
            doExecuteRefresh(dtName, triggerType);
        } catch (Exception e) {
            LOG.error("event=dt_refresh_unexpected_error dtName={} triggerType={} error={}",
                    dtName, triggerType, e.getMessage(), e);
        }
    }

    private void doExecuteRefresh(String dtName, TriggerType triggerType) {
        if (!deps.leaderCheck().getAsBoolean()) {
            LOG.debug("event=dt_refresh_skipped_not_leader dtName={} triggerType={}", dtName, triggerType);
            return;
        }
        DynamicTableState state = loadLatestState(dtName);
        if (state == null) {
            return;
        }
        executeWithSlot(dtName, state, triggerType);
    }

    private void executeWithSlot(String dtName, DynamicTableState state, TriggerType triggerType) {
        TenantScheduleQueue queue = getOrCreateQueue(state.tenantId());
        if (!tryAcquireSlots(queue)) {
            logSkippedConcurrent(dtName);
            return;
        }
        try {
            long startMs = System.currentTimeMillis();
            RefreshResult result = deps.refreshExecutor().execute(state, triggerType);
            long durationMs = System.currentTimeMillis() - startMs;
            handlePostRefresh(dtName, state, result, durationMs);
        } finally {
            releaseSlots(queue);
        }
    }

    private void handlePostRefresh(String dtName, DynamicTableState state,
                                   RefreshResult result, long durationMs) {
        emitLagMetrics(dtName, state);
        checkLagWarning(dtName, state, durationMs);
        checkQueueOverflow(state.tenantId());
        if (result.status() == RefreshStatus.FAILURE) {
            handleFailureBackoff(dtName, state);
        } else if (result.status() == RefreshStatus.SUCCESS) {
            triggerCascadeRefresh(dtName);
        }
    }

    private void triggerCascadeRefresh(String dtName) {
        DagProvider dag = deps.dagProvider();
        if (dag != null) {
            onUpstreamRefreshComplete(dtName, dag);
        }
    }

    private void handleFailureBackoff(String dtName, DynamicTableState state) {
        DynamicTableState latest = loadLatestState(dtName);
        if (latest == null) {
            return;
        }
        if (latest.state() == org.pangrova.metadata.dynamictable.DynamicTableLifecycleState.SUSPENDED) {
            unregister(dtName);
            LOG.info("event=dt_auto_suspend_unregistered dtName={} consecutiveFailures={}",
                    dtName, latest.consecutiveFailures());
            return;
        }
        scheduleBackoff(dtName, latest.consecutiveFailures());
    }

    private void scheduleBackoff(String dtName, int failures) {
        if (failures <= 0) {
            return;
        }
        long backoffMs = computeBackoffMs(failures);
        cancelFuture(dtName);
        ScheduledFuture<?> future = scheduler.schedule(
                () -> executeRefreshSafely(dtName, TriggerType.SCHEDULED),
                backoffMs, TimeUnit.MILLISECONDS);
        scheduledTasks.put(dtName, future);
        LOG.info("event=dt_refresh_backoff dtName={} failures={} backoffMs={}",
                dtName, failures, backoffMs);
    }

    private boolean tryAcquireSlots(TenantScheduleQueue queue) {
        if (queue == null || !queue.tryStartRefresh()) {
            return false;
        }
        // CAS loop to atomically claim a global slot — avoids TOCTOU race
        while (true) {
            int current = globalActiveCount.get();
            if (current >= config.maxConcurrentGlobal()) {
                queue.completeRefresh(); // release tenant slot on global rejection
                return false;
            }
            if (globalActiveCount.compareAndSet(current, current + 1)) {
                return true;
            }
        }
    }

    private void releaseSlots(TenantScheduleQueue queue) {
        globalActiveCount.decrementAndGet();
        if (queue != null) {
            queue.completeRefresh();
        }
    }

    private void emitLagMetrics(String dtName, DynamicTableState state) {
        long lagMs = state.lastRefreshTime() != null
                ? Duration.between(state.lastRefreshTime(), Instant.now()).toMillis()
                : -1;
        LOG.info("event=dt_current_lag_ms dtName={} lagMs={}", dtName, lagMs);
    }

    private void checkLagWarning(String dtName, DynamicTableState state, long durationMs) {
        long intervalMs = computeIntervalMs(state.targetLag());
        if (intervalMs > 0 && (double) durationMs / intervalMs > LAG_WARNING_RATIO) {
            LOG.warn("event=dt_lag_warning dtName={} durationMs={} intervalMs={} ratio={}",
                    dtName, durationMs, intervalMs,
                    String.format("%.2f", (double) durationMs / intervalMs));
        }
    }

    private void checkQueueOverflow(String tenantId) {
        TenantScheduleQueue queue = tenantQueues.get(tenantId);
        if (queue != null && queue.queueDepth() > config.queueOverflowThreshold()) {
            LOG.warn("event=dt_queue_overflow tenantId={} depth={} threshold={}",
                    tenantId, queue.queueDepth(), config.queueOverflowThreshold());
        }
    }

    private void logQueueMetrics(String tenantId, TenantScheduleQueue queue) {
        LOG.info("event=dt_schedule_queue_depth tenantId={} depth={} active={}",
                tenantId, queue.queueDepth(), queue.activeCount());
    }

    private void logSkippedConcurrent(String dtName) {
        LOG.info("event=dt_refresh_skipped_concurrent dtName={}", dtName);
    }

    private DynamicTableState loadLatestState(String dtName) {
        return deps.stateStore().loadState(dtName).orElse(null);
    }

    private TenantScheduleQueue getOrCreateQueue(String tenantId) {
        return tenantQueues.computeIfAbsent(tenantId,
                id -> new TenantScheduleQueue(id, config.maxConcurrentPerTenant(),
                        config.maxDynamicTablesPerTenant()));
    }

    private void cancelFuture(String dtName) {
        ScheduledFuture<?> existing = scheduledTasks.remove(dtName);
        if (existing != null) {
            existing.cancel(false);
        }
    }

    private long computeIntervalMs(Duration targetLag) {
        return Math.max(targetLag.toMillis() / 2, 1);
    }

    private long computeBackoffMs(int failureCount) {
        long base = config.retryInitialIntervalMs();
        long backoff = base * (1L << Math.min(failureCount - 1, MAX_BACKOFF_EXPONENT));
        return Math.min(backoff, config.retryMaxIntervalMs());
    }

    private static DynamicTableState withTargetLag(DynamicTableState s, Duration newLag) {
        return s.toBuilder().targetLag(newLag).updatedAt(java.time.Instant.now()).build();
    }
}
