package org.pangrova.scheduler.builtin.placement;

import org.pangrova.scheduler.api.placement.WorkerSelectionCandidate;
import org.pangrova.scheduler.api.placement.WorkerSelectionRequest;
import org.pangrova.scheduler.builtin.scoring.WorkerPerformanceScorer;
import org.pangrova.scheduler.spi.placement.WorkerSelectionStrategy;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;

/** Resource headroom ranking with deterministic, concurrent rotation of ties. */
public final class LoadBalancedStrategy implements WorkerSelectionStrategy {
    private final WorkerPerformanceScorer scorer = new WorkerPerformanceScorer();
    private final AtomicLong equalScoreCursor = new AtomicLong();

    @Override
    public Optional<String> select(WorkerSelectionRequest request) {
        double highestScore = Double.NEGATIVE_INFINITY;
        var tied = new ArrayList<WorkerSelectionCandidate>();
        for (var candidate : request.candidates()) {
            // Preserve the current ranking formula. Latencies are not reported by
            // PlacementWorker; their constant contribution cannot rank workers.
            double value = scorer.computeScore(new WorkerPerformanceScorer.WorkerStats(
                    candidate.memoryPressureRatio(), candidate.cpuUtilization(), 0, 0));
            int comparison = Double.compare(value, highestScore);
            if (comparison > 0) {
                highestScore = value;
                tied.clear();
                tied.add(candidate);
            } else if (comparison == 0) {
                tied.add(candidate);
            }
        }
        if (tied.size() <= 1) return tied.stream().findFirst().map(WorkerSelectionCandidate::workerId);
        tied.sort(Comparator.comparing(WorkerSelectionCandidate::workerId));
        int index = (int) Math.floorMod(equalScoreCursor.getAndIncrement(), tied.size());
        return Optional.of(tied.get(index).workerId());
    }
}
