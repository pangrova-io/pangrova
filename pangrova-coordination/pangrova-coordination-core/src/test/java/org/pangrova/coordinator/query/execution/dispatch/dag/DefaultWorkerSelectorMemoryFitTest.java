package org.pangrova.coordinator.query.execution.dispatch.dag;

import org.pangrova.coordinator.query.execution.split.SplitAssignmentManager;
import org.pangrova.coordinator.query.execution.split.CacheAffinityAssigner;
import org.pangrova.scheduler.core.failure.NoAvailableWorkerException;
import org.pangrova.scheduler.core.placement.TaskScheduler;
import org.pangrova.scheduler.api.placement.HealthState;
import org.pangrova.coordinator.worker.WorkerInfo;
import org.pangrova.coordinator.worker.WorkerManager;
import org.pangrova.scheduler.core.scoring.WorkerPerformanceScorer;
import org.pangrova.common.value.MemoryPressure;
import org.pangrova.coordination.inmemory.InMemoryWorkerRegistry;
import org.pangrova.spi.coordination.CacheCatalogSummary;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Regression for the fit-filter bypass in {@link DefaultWorkerSelector}: select/selectExcluding
 * previously passed a {@code 0L} estimate, which made {@code TaskScheduler.isMemoryAcceptable}
 * fall through to the pressure-only heuristic and admit workers that cannot hold even a minimal
 * task. The selector now passes the {@code MIN_TASK_MEMORY_BYTES} (64MB) floor so under-provisioned
 * workers are excluded.
 */
class DefaultWorkerSelectorMemoryFitTest {

    private static final long MIN_TASK_MEMORY_BYTES = 64L * 1024 * 1024;

    @Test
    void selectExcludesWorkerBelowMinTaskMemoryFloor() {
        WorkerManager wm = buildWorkerManager();
        // Available memory below the 64MB floor: must be excluded even though pressure is NONE.
        wm.registerWorker(worker("tiny", MIN_TASK_MEMORY_BYTES - 1));
        DefaultWorkerSelector selector = newSelector(wm);

        assertThatThrownBy(() -> selector.select(null))
                .isInstanceOf(NoAvailableWorkerException.class);
    }

    @Test
    void selectAdmitsWorkerAtOrAboveFloor() {
        WorkerManager wm = buildWorkerManager();
        wm.registerWorker(worker("ok", MIN_TASK_MEMORY_BYTES));
        DefaultWorkerSelector selector = newSelector(wm);

        WorkerInfo chosen = selector.select(null);

        assertThat(chosen.workerId()).isEqualTo("ok");
    }

    @Test
    void selectExcludingAppliesMemoryFloorToo() {
        WorkerManager wm = buildWorkerManager();
        wm.registerWorker(worker("tiny", MIN_TASK_MEMORY_BYTES - 1));
        DefaultWorkerSelector selector = newSelector(wm);

        assertThatThrownBy(() -> selector.selectExcluding(null, "some-other-worker"))
                .isInstanceOf(NoAvailableWorkerException.class);
    }

    @Test
    void registeredWorkerWithoutResourceHeartbeatDoesNotInflateStageCapacity() {
        WorkerManager wm = buildWorkerManager();
        wm.registerWorker(worker("ready", 2L * 1024 * 1024 * 1024));
        wm.registerWorker(worker("registered-only", 0));
        DefaultWorkerSelector selector = newSelector(wm);

        assertThat(wm.getActiveWorkers()).hasSize(2);
        assertThat(selector.activeWorkerCount()).isEqualTo(1);
        var grant = selector.computeResourceLimit(1, 1, wm.getWorker("ready"));
        assertThat(grant.getNativeMemoryBytes() + grant.getShuffleBufferBytes())
                .isEqualTo(2L * 1024 * 1024 * 1024);
        assertThat(grant.getShuffleBufferBytes()).isEqualTo(32L * 1024 * 1024);
    }

    @Test
    void resourceGrantFitsTheExactSelectedWorker() {
        WorkerManager wm = buildWorkerManager();
        wm.registerWorker(worker("large", 2L * 1024 * 1024 * 1024));
        wm.registerWorker(worker("small", 512L * 1024 * 1024));
        DefaultWorkerSelector selector = newSelector(wm);

        var small = selector.computeResourceLimit(1, 1, wm.getWorker("small"));
        var large = selector.computeResourceLimit(1, 1, wm.getWorker("large"));
        assertThat(small.getNativeMemoryBytes() + small.getShuffleBufferBytes())
                .isEqualTo(512L * 1024 * 1024);
        assertThat(large.getNativeMemoryBytes() + large.getShuffleBufferBytes())
                .isEqualTo(2L * 1024 * 1024 * 1024);
    }

    @Test
    void noResourceHeartbeatFailsStageGrantClosed() {
        WorkerManager wm = buildWorkerManager();
        wm.registerWorker(worker("registered-only", 0));
        DefaultWorkerSelector selector = newSelector(wm);

        assertThat(selector.activeWorkerCount()).isZero();
        assertThatThrownBy(() -> selector.computeResourceLimit(
                1, 1, wm.getWorker("registered-only")))
                .isInstanceOf(NoAvailableWorkerException.class);
    }

    @Test
    void selectedWorkerIsRevalidatedAfterItsAvailableMemoryFalls() {
        WorkerManager wm = buildWorkerManager();
        wm.registerWorker(worker("selected", 2L * 1024 * 1024 * 1024));
        WorkerInfo staleSelection = wm.getWorker("selected");
        DefaultWorkerSelector selector = newSelector(wm);
        wm.onHeartbeat("selected", new WorkerManager.HeartbeatData(
                MemoryPressure.CRITICAL, 1.0, 1, 0));

        assertThatThrownBy(() -> selector.computeResourceLimit(
                1, 1, staleSelection))
                .isInstanceOf(NoAvailableWorkerException.class);
    }

    @Test
    void cacheAffinityCannotBypassMemoryAdmission() {
        WorkerManager wm = buildWorkerManager();
        String path = "/data/cached.parquet";
        wm.registerWorker(worker(
                "tiny",
                MIN_TASK_MEMORY_BYTES - 1,
                new CacheCatalogSummary(
                        List.of(path), 1, 1, 1, 0)));
        wm.registerWorker(worker("fit", MIN_TASK_MEMORY_BYTES, null));
        CacheAffinityAssigner affinity = new CacheAffinityAssigner();
        affinity.updateTopology(wm.getActiveWorkers());
        DefaultWorkerSelector selector = new DefaultWorkerSelector(
                wm,
                new TaskScheduler(new WorkerPerformanceScorer()),
                new SplitAssignmentManager(60_000, affinity),
                new SpillBudget(0L, ""));

        WorkerInfo selected = selector.selectWithCacheAffinity(
                null, List.of(path));

        assertThat(selected.workerId()).isEqualTo("fit");
    }

    private static DefaultWorkerSelector newSelector(WorkerManager wm) {
        return new DefaultWorkerSelector(
                wm,
                new TaskScheduler(new WorkerPerformanceScorer()),
                new SplitAssignmentManager(),
                new SpillBudget(0L, ""));
    }

    private static WorkerManager buildWorkerManager() {
        return new WorkerManager(20_000, 60_000, new InMemoryWorkerRegistry());
    }

    private static WorkerInfo worker(String id, long availableMemoryBytes) {
        return worker(id, availableMemoryBytes, null);
    }

    private static WorkerInfo worker(
            String id,
            long availableMemoryBytes,
            CacheCatalogSummary cacheSummary) {
        return new WorkerInfo(
                id, "host-" + id, 8080, 9090, 9091,
                HealthState.ACTIVE, MemoryPressure.NONE, 0.1, 0,
                availableMemoryBytes, Instant.now(), 1.0,
                "default", 2, null, List.of(), cacheSummary,
                org.pangrova.coordinator.worker.TestWorkerInfoBuilder.canonicalInventory(),
                org.pangrova.coordinator.worker.TestWorkerInfoBuilder.executionIdentity(id));
    }
}
