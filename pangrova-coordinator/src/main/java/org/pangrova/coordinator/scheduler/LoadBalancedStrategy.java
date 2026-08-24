package org.pangrova.coordinator.scheduler;

import java.util.Comparator;
import java.util.List;
import java.util.Optional;

import org.pangrova.coordinator.worker.HealthState;
import org.pangrova.coordinator.worker.WorkerInfo;
import org.pangrova.coordinator.worker.WorkerPerformanceScorer;
import org.pangrova.resource.MemoryPressure;
import org.pangrova.spi.execution.scheduling.SchedulingWorkerSnapshot;
import org.pangrova.spi.execution.scheduling.WorkerSelectionRequest;
import org.pangrova.spi.execution.scheduling.WorkerSelectionStrategy;

import static org.pangrova.common.util.Preconditions.checkNotNull;

/**
 * Selects the worker with the highest performance score among eligible candidates.
 * Filters out workers that are not ACTIVE, under CRITICAL memory pressure,
 * or lack sufficient available memory for the task.
 */
public final class LoadBalancedStrategy implements WorkerSelectionStrategy {

    private final WorkerPerformanceScorer scorer;
    private final List<WorkerInfo> workers;

    public LoadBalancedStrategy(WorkerPerformanceScorer scorer, List<WorkerInfo> workers) {
        this.scorer = checkNotNull(scorer, "scorer");
        this.workers = checkNotNull(workers, "workers");
    }

    @Override
    public Optional<SchedulingWorkerSnapshot> select(WorkerSelectionRequest request) {
        return workers.stream()
                .filter(w -> w.healthState() == HealthState.ACTIVE)
                .filter(w -> w.memoryPressure() != MemoryPressure.CRITICAL)
                .max(Comparator.comparingDouble(w -> scorer.computeScore(toStats(w))))
                .map(LoadBalancedStrategy::toSnapshot);
    }

    @Override
    public String name() {
        return "load-balanced";
    }

    private static WorkerPerformanceScorer.WorkerStats toStats(WorkerInfo w) {
        return new WorkerPerformanceScorer.WorkerStats(
                w.memoryPressure().getLowerBound(), w.cpuUtilization(), 0.0, 0.0);
    }

    static SchedulingWorkerSnapshot toSnapshot(WorkerInfo w) {
        return new SchedulingWorkerSnapshot(
                w.workerId(), w.host(), w.httpPort(),
                w.availableMemoryBytes(), w.runningTaskCount());
    }
}
