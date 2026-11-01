package org.pangrova.scheduler.strategy.loadbalanced;

import java.util.Comparator;
import java.util.List;
import java.util.Optional;

import org.pangrova.scheduler.api.placement.HealthState;
import org.pangrova.scheduler.api.placement.PlacementWorker;
import org.pangrova.scheduler.scoring.WorkerPerformanceScorer;
import org.pangrova.common.value.MemoryPressure;
import org.pangrova.scheduler.api.placement.SchedulingWorkerSnapshot;
import org.pangrova.scheduler.api.placement.WorkerSelectionRequest;
import org.pangrova.scheduler.api.placement.WorkerSelectionStrategy;

import static org.pangrova.common.util.Preconditions.checkNotNull;

/**
 * Selects the worker with the highest performance score among eligible candidates.
 * Filters out workers that are not ACTIVE, under CRITICAL memory pressure,
 * or lack sufficient available memory for the task.
 */
public final class LoadBalancedStrategy implements WorkerSelectionStrategy {

    private final WorkerPerformanceScorer scorer;
    private final List<PlacementWorker> workers;

    public LoadBalancedStrategy(WorkerPerformanceScorer scorer, List<? extends PlacementWorker> workers) {
        this.scorer = checkNotNull(scorer, "scorer");
        this.workers = List.copyOf(checkNotNull(workers, "workers"));
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

    private static WorkerPerformanceScorer.WorkerStats toStats(PlacementWorker w) {
        return new WorkerPerformanceScorer.WorkerStats(
                w.memoryPressure().getLowerBound(), w.cpuUtilization(), 0.0, 0.0);
    }

    public static SchedulingWorkerSnapshot toSnapshot(PlacementWorker w) {
        return new SchedulingWorkerSnapshot(
                w.workerId(), w.host(), w.httpPort(),
                w.availableMemoryBytes(), w.runningTaskCount());
    }
}
