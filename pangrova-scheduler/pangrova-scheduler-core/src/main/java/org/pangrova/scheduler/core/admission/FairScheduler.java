package org.pangrova.scheduler.core.admission;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.pangrova.scheduler.core.resource.QueueType;

/**
 * Capacity-based fair scheduler for query admission per queue type.
 */
public class FairScheduler {

    private static final Map<QueueType, Integer> DEFAULT_CAPACITIES = Map.of(
            QueueType.INTERACTIVE, 100,
            QueueType.BATCH, 50,
            QueueType.BACKGROUND, 20
    );

    private final ConcurrentHashMap<QueueType, AtomicInteger> queueUsage = new ConcurrentHashMap<>();
    private final Map<QueueType, Integer> queueCapacities;

    public FairScheduler() {
        this(DEFAULT_CAPACITIES);
    }

    /** Creates a scheduler with explicit per-queue capacities. */
    public FairScheduler(Map<QueueType, Integer> capacities) {
        this.queueCapacities = Map.copyOf(capacities);
        for (QueueType type : QueueType.values()) {
            queueUsage.put(type, new AtomicInteger(0));
        }
    }

    /**
     * Attempts to admit a query into the specified queue.
     * Returns false if the queue is at capacity.
     */
    public boolean submitQuery(String queryId, QueueType queueType) {
        int capacity = queueCapacities.getOrDefault(queueType, 0);
        AtomicInteger usage = queueUsage.get(queueType);
        return usage != null && usage.getAndUpdate(v -> v < capacity ? v + 1 : v) < capacity;
    }

    /** Releases one admitted slot for the query's queue. */
    public void completeQuery(String queryId, QueueType queueType) {
        AtomicInteger usage = queueUsage.get(queueType);
        if (usage != null) {
            usage.getAndUpdate(v -> Math.max(0, v - 1));
        }
    }

    public int getQueueUsage(QueueType queueType) {
        AtomicInteger usage = queueUsage.get(queueType);
        return usage != null ? usage.get() : 0;
    }
}
