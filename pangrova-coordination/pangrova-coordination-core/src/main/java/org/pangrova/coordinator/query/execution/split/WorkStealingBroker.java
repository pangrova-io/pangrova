package org.pangrova.coordinator.query.execution.split;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import org.pangrova.common.api.annotation.ThreadSafe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Work-stealing broker: idle workers request splits from busy workers.
 *
 * <p>When a worker finishes its assigned splits and the central queue is empty,
 * it can steal from the busiest worker's pending queue. The broker tracks
 * per-worker pending counts and selects the steal victim with the most work.</p>
 *
 * <p>Steal protocol:
 * <ol>
 *   <li>Idle worker calls {@link #requestSteal(String, String, int)}</li>
 *   <li>Broker finds the busiest worker with pending splits &gt; threshold</li>
 *   <li>Transfers half of the victim's pending splits to the thief</li>
 *   <li>Returns transferred splits; empty if no victim is eligible</li>
 * </ol>
 *
 * <p>Thread-safe: uses ConcurrentHashMap for worker queues.</p>
 */
@ThreadSafe
public final class WorkStealingBroker {

    private static final Logger LOG = LoggerFactory.getLogger(WorkStealingBroker.class);

    /** Minimum pending splits on a worker to be eligible as a steal victim. */
    private static final int MIN_STEAL_THRESHOLD = 2;

    private final ConcurrentHashMap<String, WorkerSplitTracker> workerTrackers = new ConcurrentHashMap<>();
    private final AtomicLong totalSteals = new AtomicLong(0);

    /**
     * Registers pending split count for a worker during initial assignment.
     */
    public void registerWorkerSplits(String queryId, int stageId, String workerId, int pendingCount) {
        String key = buildKey(queryId, stageId, workerId);
        workerTrackers.put(key, new WorkerSplitTracker(workerId, pendingCount));
        LOG.debug("event=worker_splits_registered key={} pending={}", key, pendingCount);
    }

    /**
     * Decrements pending count when a worker completes a split.
     */
    public void onSplitCompleted(String queryId, int stageId, String workerId) {
        String key = buildKey(queryId, stageId, workerId);
        WorkerSplitTracker tracker = workerTrackers.get(key);
        if (tracker != null) {
            tracker.decrementPending();
        }
    }

    /**
     * Attempts to steal work from the busiest worker for the given stage.
     *
     * @param queryId  the query identifier
     * @param stageId  the stage identifier
     * @param thiefId  the idle worker requesting work
     * @return the number of splits transferred (0 if no steal occurred)
     */
    public int requestSteal(String queryId, int stageId, String thiefId) {
        String stagePrefix = queryId + ":" + stageId + ":";
        Optional<Map.Entry<String, WorkerSplitTracker>> victim = workerTrackers.entrySet().stream()
                .filter(e -> e.getKey().startsWith(stagePrefix))
                .filter(e -> !e.getValue().workerId().equals(thiefId))
                .filter(e -> e.getValue().pendingCount() >= MIN_STEAL_THRESHOLD)
                .max((a, b) -> Integer.compare(a.getValue().pendingCount(), b.getValue().pendingCount()));

        if (victim.isEmpty()) {
            return 0;
        }

        WorkerSplitTracker victimTracker = victim.get().getValue();
        int stolen = victimTracker.pendingCount() / 2;
        if (stolen <= 0) {
            return 0;
        }

        victimTracker.transferSplits(stolen);
        String thiefKey = buildKey(queryId, stageId, thiefId);
        workerTrackers.computeIfAbsent(thiefKey, k -> new WorkerSplitTracker(thiefId, 0))
                .addPending(stolen);

        totalSteals.addAndGet(stolen);
        LOG.info("event=work_steal queryId={} stageId={} thief={} victim={} stolen={}",
                queryId, stageId, thiefId, victimTracker.workerId(), stolen);
        return stolen;
    }

    /** Returns total splits stolen across all queries. */
    public long totalSteals() {
        return totalSteals.get();
    }

    /** Cleans up tracking for a completed query. */
    public void cleanup(String queryId) {
        workerTrackers.keySet().removeIf(k -> k.startsWith(queryId + ":"));
    }

    private static String buildKey(String queryId, int stageId, String workerId) {
        return queryId + ":" + stageId + ":" + workerId;
    }

    /**
     * Tracks pending split count for a single worker in a stage.
     */
    static final class WorkerSplitTracker {
        private final String workerId;
        private final java.util.concurrent.atomic.AtomicInteger pendingCount;

        WorkerSplitTracker(String workerId, int pendingCount) {
            this.workerId = workerId;
            this.pendingCount = new java.util.concurrent.atomic.AtomicInteger(pendingCount);
        }

        String workerId() { return workerId; }
        int pendingCount() { return pendingCount.get(); }

        void decrementPending() {
            pendingCount.updateAndGet(current -> Math.max(0, current - 1));
        }

        void transferSplits(int count) {
            pendingCount.updateAndGet(current -> Math.max(0, current - count));
        }

        void addPending(int count) {
            pendingCount.addAndGet(count);
        }
    }
}
