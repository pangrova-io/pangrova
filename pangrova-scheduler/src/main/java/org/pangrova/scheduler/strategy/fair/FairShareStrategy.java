package org.pangrova.scheduler.strategy.fair;

import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import org.pangrova.scheduler.api.placement.HealthState;
import org.pangrova.scheduler.api.placement.PlacementWorker;
import org.pangrova.scheduler.scoring.WorkerPerformanceScorer;
import org.pangrova.common.value.MemoryPressure;
import org.pangrova.scheduler.resource.QueueType;
import org.pangrova.scheduler.resource.WeightedFairScheduler;
import org.pangrova.scheduler.strategy.loadbalanced.LoadBalancedStrategy;
import org.pangrova.scheduler.api.placement.SchedulingWorkerSnapshot;
import org.pangrova.scheduler.api.placement.WorkerSelectionRequest;
import org.pangrova.scheduler.api.placement.WorkerSelectionStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.pangrova.common.util.Preconditions.checkNotNull;

/**
 * Selects workers using weighted fair share scoring.
 * The tenant's queue type share is multiplied with the performance score
 * to bias selection toward workers serving under-utilized tenant queues.
 */
public final class FairShareStrategy implements WorkerSelectionStrategy {

    private static final Logger LOG = LoggerFactory.getLogger(FairShareStrategy.class);
    private static final double SHARE_FLOOR = 0.1;

    private static final QueueType DEFAULT_QUEUE_TYPE = QueueType.BATCH;
    private static final String INTERACTIVE_PREFIX = "interactive";
    private static final String BACKGROUND_PREFIX = "background";

    private final WorkerPerformanceScorer scorer;
    private final WeightedFairScheduler fairScheduler;
    private final List<PlacementWorker> workers;

    public FairShareStrategy(WorkerPerformanceScorer scorer,
                             WeightedFairScheduler fairScheduler,
                             List<? extends PlacementWorker> workers) {
        this.scorer = checkNotNull(scorer, "scorer");
        this.fairScheduler = checkNotNull(fairScheduler, "fairScheduler");
        this.workers = List.copyOf(checkNotNull(workers, "workers"));
    }

    @Override
    public Optional<SchedulingWorkerSnapshot> select(WorkerSelectionRequest request) {
        QueueType queueType = resolveQueueType(request.tenantId());
        Set<QueueType> activeQueues = fairScheduler.getActiveQueues();
        if (activeQueues.isEmpty()) {
            activeQueues = Set.of(QueueType.values());
        }
        double share = Math.max(fairScheduler.computeShare(queueType, activeQueues), SHARE_FLOOR);
        LOG.debug("event=fair_share_selection tenantId={} queueType={} share={}",
                request.tenantId(), queueType, share);

        final double effectiveShare = share;
        return workers.stream()
                .filter(w -> w.healthState() == HealthState.ACTIVE)
                .filter(w -> w.memoryPressure() != MemoryPressure.CRITICAL)
                .max(Comparator.comparingDouble(w ->
                        scorer.computeScore(toStats(w)) * effectiveShare))
                .map(LoadBalancedStrategy::toSnapshot);
    }

    @Override
    public String name() {
        return "fair-share";
    }

    /**
     * Maps tenant identifier to queue type using prefix convention.
     * Constants are defined at class level for maintainability.
     */
    private static QueueType resolveQueueType(String tenantId) {
        if (tenantId == null) {
            return DEFAULT_QUEUE_TYPE;
        }
        if (tenantId.startsWith(INTERACTIVE_PREFIX)) {
            return QueueType.INTERACTIVE;
        }
        if (tenantId.startsWith(BACKGROUND_PREFIX)) {
            return QueueType.BACKGROUND;
        }
        return DEFAULT_QUEUE_TYPE;
    }

    private static WorkerPerformanceScorer.WorkerStats toStats(PlacementWorker w) {
        return new WorkerPerformanceScorer.WorkerStats(
                w.memoryPressure().getLowerBound(), w.cpuUtilization(), 0.0, 0.0);
    }
}
