package org.pangrova.coordinator.query.execution.dispatch.dag;

import org.pangrova.coordinator.query.execution.split.SplitAssignmentManager;
import org.pangrova.coordinator.scheduler.NoAvailableWorkerException;
import org.pangrova.coordinator.scheduler.TaskScheduler;
import org.pangrova.coordinator.worker.HealthState;
import org.pangrova.coordinator.worker.WorkerInfo;
import org.pangrova.coordinator.worker.WorkerManager;
import org.pangrova.coordinator.worker.WorkerPerformanceScorer;
import org.pangrova.resource.MemoryPressure;
import org.pangrova.spi.coordination.inmemory.InMemoryWorkerRegistry;
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
        return new WorkerInfo(
                id, "host-" + id, 8080, 9090, 9091,
                HealthState.ACTIVE, MemoryPressure.NONE, 0.1, 0,
                availableMemoryBytes, Instant.now(), 1.0,
                "default", 1, List.of(), null, List.of());
    }
}
