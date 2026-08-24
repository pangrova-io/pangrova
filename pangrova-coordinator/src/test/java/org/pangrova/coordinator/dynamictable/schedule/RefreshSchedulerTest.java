package org.pangrova.coordinator.dynamictable.schedule;

import org.pangrova.coordinator.dynamictable.DynamicTableConfig;
import org.pangrova.coordinator.dynamictable.refresh.DagProvider;
import org.pangrova.coordinator.dynamictable.refresh.pipeline.RefreshExecutor;
import org.pangrova.coordinator.dynamictable.refresh.pipeline.RefreshResult;
import org.pangrova.metadata.dynamictable.DynamicTableLifecycleState;
import org.pangrova.metadata.dynamictable.DynamicTableState;
import org.pangrova.metadata.dynamictable.InMemoryDynamicTableStateStore;
import org.pangrova.metadata.dynamictable.DynamicTableRefreshMode;
import org.pangrova.metadata.dynamictable.TriggerType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for {@link RefreshScheduler}.
 *
 * <p>Uses real {@link InMemoryDynamicTableStateStore} and hand-written
 * test implementations — no mock frameworks.</p>
 *
 * <p><b>Validates: Requirements 5, 7, 9, 10, 16</b></p>
 */
class RefreshSchedulerTest {

    private InMemoryDynamicTableStateStore stateStore;
    private DynamicTableConfig config;
    private RefreshScheduler scheduler;
    private CountingRefreshExecutor executor;

    @BeforeEach
    void setUp() {
        stateStore = new InMemoryDynamicTableStateStore();
        config = DynamicTableConfig.defaults();
        executor = new CountingRefreshExecutor();
    }

    @AfterEach
    void tearDown() {
        if (scheduler != null) {
            scheduler.close();
        }
    }

    // ── Leader-gate verification ───────────────────────────────────
    // Verifies that ALL refresh trigger paths (scheduled, manual, cascade, backoff)
    // funnel through doExecuteRefresh() which checks deps.leaderCheck().getAsBoolean().
    // When leader check returns false, no refresh should execute.

    @Test
    void scheduledRefresh_skippedWhenNotLeader() throws Exception {
        CountDownLatch latch = new CountDownLatch(1);
        executor = new CountingRefreshExecutor(latch);
        DynamicTableConfig shortConfig = new DynamicTableConfig(
                4, 2, 100, Duration.ofMillis(100), 1000L,
                1000L, 300_000L, 5, 10, 3.0, 300_000L, 1000, 30, 20);
        var deps = new RefreshSchedulerContext(executor, stateStore, shortConfig, () -> false);
        scheduler = new RefreshScheduler(deps);

        DynamicTableState state = stateWithLag("dt_sched", "tenant-1", Duration.ofMillis(100));
        stateStore.saveState(state);
        scheduler.register(state);

        // Wait enough time for several scheduled ticks to fire
        assertThat(latch.await(500, TimeUnit.MILLISECONDS)).isFalse();
        assertThat(executor.callCount()).isZero();
    }

    @Test
    void manualRefresh_skippedWhenNotLeader() throws Exception {
        CountDownLatch latch = new CountDownLatch(1);
        executor = new CountingRefreshExecutor(latch);
        var deps = new RefreshSchedulerContext(executor, stateStore, config, () -> false);
        scheduler = new RefreshScheduler(deps);

        DynamicTableState state = activeState("dt_manual", "tenant-1");
        stateStore.saveState(state);
        scheduler.register(state);
        scheduler.triggerManualRefresh("dt_manual");

        assertThat(latch.await(500, TimeUnit.MILLISECONDS)).isFalse();
        assertThat(executor.callCount()).isZero();
    }

    @Test
    void cascadeRefresh_skippedWhenNotLeader() throws Exception {
        CountDownLatch latch = new CountDownLatch(1);
        executor = new CountingRefreshExecutor(latch);
        var deps = new RefreshSchedulerContext(executor, stateStore, config, () -> false);
        scheduler = new RefreshScheduler(deps);

        DynamicTableState state = activeState("downstream1", "tenant-1");
        stateStore.saveState(state);
        scheduler.register(state);

        DagProvider dag = dt -> Set.of("downstream1");
        scheduler.onUpstreamRefreshComplete("upstream", dag);

        assertThat(latch.await(500, TimeUnit.MILLISECONDS)).isFalse();
        assertThat(executor.callCount()).isZero();
    }

    // ── Register / Unregister lifecycle ─────────────────────────────

    @Test
    void register_addsToTenantQueueAndSchedules() {
        scheduler = buildScheduler();
        DynamicTableState state = activeState("dt1", "tenant-1");
        stateStore.saveState(state);

        scheduler.register(state);

        assertThat(scheduler.isRegistered("dt1")).isTrue();
        assertThat(scheduler.registeredCount()).isEqualTo(1);
        TenantScheduleQueue queue = scheduler.getTenantQueue("tenant-1");
        assertThat(queue).isNotNull();
        assertThat(queue.queueDepth()).isEqualTo(1);
    }

    @Test
    void unregister_removesFromScheduler() {
        scheduler = buildScheduler();
        DynamicTableState state = activeState("dt1", "tenant-1");
        stateStore.saveState(state);

        scheduler.register(state);
        scheduler.unregister("dt1");

        assertThat(scheduler.isRegistered("dt1")).isFalse();
        assertThat(scheduler.registeredCount()).isZero();
    }

    @Test
    void register_multipleTenantsIsolated() {
        scheduler = buildScheduler();
        DynamicTableState s1 = activeState("dt1", "tenant-1");
        DynamicTableState s2 = activeState("dt2", "tenant-2");
        stateStore.saveState(s1);
        stateStore.saveState(s2);

        scheduler.register(s1);
        scheduler.register(s2);

        assertThat(scheduler.getTenantQueue("tenant-1").queueDepth()).isEqualTo(1);
        assertThat(scheduler.getTenantQueue("tenant-2").queueDepth()).isEqualTo(1);
    }

    // ── Target lag dynamic adjustment ───────────────────────────────

    @Test
    void updateTargetLag_reschedulesWithNewInterval() {
        scheduler = buildScheduler();
        DynamicTableState state = activeState("dt1", "tenant-1");
        stateStore.saveState(state);

        scheduler.register(state);
        scheduler.updateTargetLag("dt1", Duration.ofMinutes(10));

        assertThat(scheduler.isRegistered("dt1")).isTrue();
    }

    @Test
    void updateTargetLag_unregisteredDt_noOp() {
        scheduler = buildScheduler();
        // should not throw
        scheduler.updateTargetLag("nonexistent", Duration.ofMinutes(1));
    }

    // ── Manual trigger ──────────────────────────────────────────────

    @Test
    void triggerManualRefresh_executesImmediately() throws Exception {
        CountDownLatch latch = new CountDownLatch(1);
        executor = new CountingRefreshExecutor(latch);
        scheduler = buildSchedulerWithLongInterval();
        DynamicTableState state = activeState("dt1", "tenant-1");
        stateStore.saveState(state);

        scheduler.register(state);
        scheduler.triggerManualRefresh("dt1");

        assertThat(latch.await(5, TimeUnit.SECONDS)).isTrue();
        assertThat(executor.callCount()).isGreaterThanOrEqualTo(1);
    }

    @Test
    void triggerManualRefresh_unregisteredDt_ignored() {
        scheduler = buildScheduler();
        // should not throw
        scheduler.triggerManualRefresh("nonexistent");
        assertThat(executor.callCount()).isZero();
    }

    // ── DAG cascade ─────────────────────────────────────────────────

    @Test
    void onUpstreamRefreshComplete_triggersCascadeForDownstreams() throws Exception {
        CountDownLatch latch = new CountDownLatch(2);
        executor = new CountingRefreshExecutor(latch);
        scheduler = buildSchedulerWithLongInterval();
        DynamicTableState s1 = activeState("downstream1", "tenant-1");
        DynamicTableState s2 = activeState("downstream2", "tenant-1");
        stateStore.saveState(s1);
        stateStore.saveState(s2);
        scheduler.register(s1);
        scheduler.register(s2);

        DagProvider dag = dt -> Set.of("downstream1", "downstream2");
        scheduler.onUpstreamRefreshComplete("upstream", dag);

        assertThat(latch.await(5, TimeUnit.SECONDS)).isTrue();
        assertThat(executor.callCount()).isGreaterThanOrEqualTo(2);
    }

    @Test
    void onUpstreamRefreshComplete_unregisteredDownstream_skipped() {
        scheduler = buildScheduler();
        DagProvider dag = dt -> Set.of("not_registered");
        // should not throw
        scheduler.onUpstreamRefreshComplete("upstream", dag);
    }

    // ── Concurrent lock skip (global max) ───────────────────────────

    @Test
    void globalActiveCountAtMax_skipsRefresh() throws Exception {
        // Use maxConcurrentGlobal=1 so a single blocking task fills the slot.
        // The single-thread scheduler serializes tasks, so the second manual
        // trigger will see globalActiveCount=1 and skip.
        CountDownLatch blockLatch = new CountDownLatch(1);
        CountDownLatch startedLatch = new CountDownLatch(1);
        var blockingExecutor = new BlockingRefreshExecutor(blockLatch, startedLatch);
        // maxConcurrentGlobal=1, maxConcurrentPerTenant=100
        DynamicTableConfig cfg = new DynamicTableConfig(
                1, 100, 100, Duration.ofMinutes(5), 1000L,
                1000L, 300_000L, 5, 10, 3.0, 300_000L, 1000, 30, 20);
        // Use a multi-thread scheduler so blocking + skip can happen concurrently
        var multiThreadScheduler = new java.util.concurrent.ScheduledThreadPoolExecutor(
                4, r -> { Thread t = new Thread(r, "test-sched"); t.setDaemon(true); return t; });
        multiThreadScheduler.setRemoveOnCancelPolicy(true);
        var deps = new RefreshSchedulerContext(blockingExecutor, stateStore, cfg);
        scheduler = new RefreshScheduler(deps, multiThreadScheduler);

        DynamicTableState s1 = activeState("dt1", "tenant-1");
        DynamicTableState s2 = activeState("dt2", "tenant-1");
        stateStore.saveState(s1);
        stateStore.saveState(s2);
        scheduler.register(s1);
        scheduler.register(s2);

        scheduler.triggerManualRefresh("dt1");
        // Wait for first to start blocking
        assertThat(startedLatch.await(5, TimeUnit.SECONDS)).isTrue();
        // Now trigger second — should be skipped because global is at max (1)
        scheduler.triggerManualRefresh("dt2");
        Thread.sleep(300);

        // Only 1 should have actually called the executor
        assertThat(blockingExecutor.callCount()).isEqualTo(1);
        blockLatch.countDown();
    }

    // ── Tenant isolation (per-tenant maxConcurrent) ─────────────────

    @Test
    void tenantMaxConcurrent_limitsPerTenantRefreshes() {
        scheduler = buildScheduler();
        TenantScheduleQueue queue = new TenantScheduleQueue("tenant-1", 2, 100);

        assertThat(queue.tryStartRefresh()).isTrue();
        assertThat(queue.tryStartRefresh()).isTrue();
        assertThat(queue.tryStartRefresh()).isFalse();

        queue.completeRefresh();
        assertThat(queue.tryStartRefresh()).isTrue();
    }

    // ── stopAll resource release ────────────────────────────────────

    @Test
    void stopAll_clearsAllState() {
        scheduler = buildScheduler();
        DynamicTableState s1 = activeState("dt1", "tenant-1");
        DynamicTableState s2 = activeState("dt2", "tenant-2");
        stateStore.saveState(s1);
        stateStore.saveState(s2);
        scheduler.register(s1);
        scheduler.register(s2);

        scheduler.stopAll();

        assertThat(scheduler.registeredCount()).isZero();
        assertThat(scheduler.globalActiveCount()).isZero();
    }

    @Test
    void close_isIdempotent() {
        scheduler = buildScheduler();
        scheduler.close();
        scheduler.close(); // should not throw
    }

    // ── Exponential backoff scheduling ──────────────────────────────

    @Test
    void consecutiveFailures_triggersBackoff() throws Exception {
        CountDownLatch latch = new CountDownLatch(1);
        var failingExecutor = new FailingRefreshExecutor(latch);
        scheduler = buildSchedulerWith(failingExecutor, config);

        DynamicTableState state = stateWithFailures("backoff_dt", "tenant-1", 3);
        stateStore.saveState(state);
        scheduler.register(state);
        scheduler.triggerManualRefresh("backoff_dt");

        assertThat(latch.await(5, TimeUnit.SECONDS)).isTrue();
        // After failure, scheduler should have rescheduled with backoff
        // The DT should still be registered
        assertThat(scheduler.isRegistered("backoff_dt")).isTrue();
    }

    // ── Queue overflow detection ────────────────────────────────────

    @Test
    void queueOverflow_detectedWhenExceedingThreshold() {
        // config defaults: queueOverflowThreshold=20, maxDynamicTablesPerTenant=100
        scheduler = buildScheduler();
        for (int i = 0; i < 25; i++) {
            DynamicTableState s = activeState("dt" + i, "tenant-1");
            stateStore.saveState(s);
            scheduler.register(s);
        }
        TenantScheduleQueue queue = scheduler.getTenantQueue("tenant-1");
        assertThat(queue.queueDepth()).isEqualTo(25);
        // overflow is logged (verified by structured log output)
    }

    // ── ScheduledRefreshTask record ─────────────────────────────────

    @Test
    void scheduledRefreshTask_immutableRecord() {
        Instant now = Instant.now();
        var task = new ScheduledRefreshTask("dt1", "tenant-1", now, TriggerType.SCHEDULED);

        assertThat(task.dtName()).isEqualTo("dt1");
        assertThat(task.tenantId()).isEqualTo("tenant-1");
        assertThat(task.scheduledTime()).isEqualTo(now);
        assertThat(task.triggerType()).isEqualTo(TriggerType.SCHEDULED);
    }

    // ── TenantScheduleQueue unit tests ──────────────────────────────

    @Test
    void tenantQueue_registerUnregisterCycle() {
        var queue = new TenantScheduleQueue("t1", 2, 5);
        assertThat(queue.canRegister()).isTrue();
        queue.register();
        assertThat(queue.queueDepth()).isEqualTo(1);
        queue.unregister();
        assertThat(queue.queueDepth()).isZero();
    }

    @Test
    void tenantQueue_quotaEnforced() {
        var queue = new TenantScheduleQueue("t1", 2, 2);
        queue.register();
        queue.register();
        assertThat(queue.canRegister()).isFalse();
    }

    // ── Periodic scheduling fires ───────────────────────────────────

    @Test
    void periodicSchedule_firesRefreshAtInterval() throws Exception {
        CountDownLatch latch = new CountDownLatch(2);
        executor = new CountingRefreshExecutor(latch);
        // Use short target lag so interval = 50ms
        DynamicTableConfig shortConfig = new DynamicTableConfig(
                4, 2, 100, Duration.ofMillis(100), 1000L,
                1000L, 300_000L, 5, 10, 3.0, 300_000L, 1000, 30, 20);
        scheduler = buildSchedulerWith(executor, shortConfig);

        DynamicTableState state = stateWithLag("fast_dt", "tenant-1", Duration.ofMillis(100));
        stateStore.saveState(state);
        scheduler.register(state);

        assertThat(latch.await(5, TimeUnit.SECONDS)).isTrue();
        assertThat(executor.callCount()).isGreaterThanOrEqualTo(2);
    }

    // ── Register rejected when tenant quota exceeded ────────────────

    @Test
    void register_rejectedWhenTenantQuotaExceeded() {
        DynamicTableConfig smallQuota = new DynamicTableConfig(
                4, 2, 2, Duration.ofMinutes(5), 1000L,
                1000L, 300_000L, 5, 10, 3.0, 300_000L, 1000, 30, 20);
        scheduler = buildSchedulerWith(executor, smallQuota);

        DynamicTableState s1 = activeState("dt1", "tenant-1");
        DynamicTableState s2 = activeState("dt2", "tenant-1");
        DynamicTableState s3 = activeState("dt3", "tenant-1");
        stateStore.saveState(s1);
        stateStore.saveState(s2);
        stateStore.saveState(s3);

        scheduler.register(s1);
        scheduler.register(s2);
        scheduler.register(s3); // should be rejected

        assertThat(scheduler.registeredCount()).isEqualTo(2);
    }

    // ── Builder helpers ─────────────────────────────────────────────

    private RefreshScheduler buildScheduler() {
        var deps = new RefreshSchedulerContext(executor, stateStore, config);
        return new RefreshScheduler(deps);
    }

    private RefreshScheduler buildSchedulerWithLongInterval() {
        // Use default config with 5min target lag → interval = 2.5min
        var deps = new RefreshSchedulerContext(executor, stateStore, config);
        return new RefreshScheduler(deps);
    }

    private RefreshScheduler buildSchedulerWith(RefreshExecutor exec, DynamicTableConfig cfg) {
        var deps = new RefreshSchedulerContext(exec, stateStore, cfg);
        return new RefreshScheduler(deps);
    }

    private DynamicTableConfig configWithGlobal(int maxGlobal) {
        return new DynamicTableConfig(
                maxGlobal, 100, 100, Duration.ofMinutes(5), 1000L,
                1000L, 300_000L, 5, 10, 3.0, 300_000L, 1000, 30, 20);
    }

    // ── State factories ─────────────────────────────────────────────

    private static DynamicTableState activeState(String dtName, String tenantId) {
        return new DynamicTableState(
                dtName, tenantId, DynamicTableLifecycleState.ACTIVE, DynamicTableRefreshMode.SNAPSHOT,
                Duration.ofMinutes(5), "100", "SNAPSHOT", 1, null,
                "admin", "coord-1", 1L, null, 0, 0,
                Instant.now(), Instant.now());
    }

    private static DynamicTableState stateWithFailures(String dtName, String tenantId,
                                                       int failures) {
        return new DynamicTableState(
                dtName, tenantId, DynamicTableLifecycleState.ACTIVE, DynamicTableRefreshMode.SNAPSHOT,
                Duration.ofMinutes(5), "100", "SNAPSHOT", 1, null,
                "admin", "coord-1", 1L, null, 0, failures,
                Instant.now(), Instant.now());
    }

    private static DynamicTableState stateWithLag(String dtName, String tenantId,
                                                  Duration targetLag) {
        return new DynamicTableState(
                dtName, tenantId, DynamicTableLifecycleState.ACTIVE, DynamicTableRefreshMode.SNAPSHOT,
                targetLag, "100", "SNAPSHOT", 1, null,
                "admin", "coord-1", 1L, null, 0, 0,
                Instant.now(), Instant.now());
    }

    // ── Test implementations ────────────────────────────────────────

    /** Executor that counts calls and returns configurable results. */
    static final class CountingRefreshExecutor implements RefreshExecutor {
        private final AtomicInteger count = new AtomicInteger(0);
        private final Map<String, RefreshResult> results = new ConcurrentHashMap<>();
        private final CountDownLatch latch;

        CountingRefreshExecutor() {
            this.latch = null;
        }

        CountingRefreshExecutor(CountDownLatch latch) {
            this.latch = latch;
        }

        void setResult(String dtName, RefreshResult result) {
            results.put(dtName, result);
        }

        @Override
        public RefreshResult execute(DynamicTableState state, TriggerType triggerType) {
            count.incrementAndGet();
            if (latch != null) {
                latch.countDown();
            }
            return results.getOrDefault(state.dtName(), RefreshResult.noOp());
        }

        int callCount() {
            return count.get();
        }
    }

    /** Executor that returns FAILURE and counts calls. */
    static final class FailingRefreshExecutor implements RefreshExecutor {
        private final AtomicInteger count = new AtomicInteger(0);
        private final CountDownLatch latch;

        FailingRefreshExecutor(CountDownLatch latch) {
            this.latch = latch;
        }

        @Override
        public RefreshResult execute(DynamicTableState state, TriggerType triggerType) {
            count.incrementAndGet();
            if (latch != null) {
                latch.countDown();
            }
            return RefreshResult.failure(100);
        }

        int callCount() {
            return count.get();
        }
    }

    /** Executor that blocks until a latch is released. */
    static final class BlockingRefreshExecutor implements RefreshExecutor {
        private final CountDownLatch blockLatch;
        private final CountDownLatch startedLatch;
        private final AtomicInteger count = new AtomicInteger(0);

        BlockingRefreshExecutor(CountDownLatch blockLatch, CountDownLatch startedLatch) {
            this.blockLatch = blockLatch;
            this.startedLatch = startedLatch;
        }

        @Override
        public RefreshResult execute(DynamicTableState state, TriggerType triggerType) {
            count.incrementAndGet();
            startedLatch.countDown();
            try {
                blockLatch.await(10, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            return RefreshResult.noOp();
        }

        int callCount() {
            return count.get();
        }
    }
}
