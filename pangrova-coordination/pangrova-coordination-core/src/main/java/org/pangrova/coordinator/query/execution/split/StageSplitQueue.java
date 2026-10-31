package org.pangrova.coordinator.query.execution.split;

import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;

import org.pangrova.common.concurrent.ThreadSafe;

/**
 * Thread-safe queue of splits for a single stage.
 * Supports locality-aware polling, lease-based assignment, and completion tracking.
 */
@ThreadSafe
public final class StageSplitQueue {
    private static final long DEFAULT_LEASE_DURATION_MS = 60_000L;

    private final ConcurrentLinkedQueue<SplitAssignment> pending;
    private final ConcurrentHashMap<Integer, SplitLease> activeLeases = new ConcurrentHashMap<>();
    private final AtomicInteger totalSplits;
    private final AtomicInteger completedSplits = new AtomicInteger(0);
    private final long leaseDurationMs;
    private final long stealDelayMs;

    public StageSplitQueue(List<SplitAssignment> splits, long leaseDurationMs) {
        this(splits, leaseDurationMs, 0);
    }

    /** Creates a stage queue with its split lease and steal delays. */
    public StageSplitQueue(List<SplitAssignment> splits, long leaseDurationMs, long stealDelayMs) {
        this.pending = new ConcurrentLinkedQueue<>(splits);
        this.totalSplits = new AtomicInteger(splits.size());
        this.leaseDurationMs = leaseDurationMs > 0 ? leaseDurationMs : DEFAULT_LEASE_DURATION_MS;
        this.stealDelayMs = stealDelayMs >= 0 ? stealDelayMs : 0;
    }

    public StageSplitQueue(List<SplitAssignment> splits) {
        this(splits, DEFAULT_LEASE_DURATION_MS, 0);
    }

    /**
     * Polls a split whose files hash to the requesting worker via consistent hash ring.
     * Priority 1 in the 3-tier assignment strategy.
     */
    public Optional<SplitAssignment> pollCacheAffineSplit(String workerId, CacheAffinityAssigner assigner) {
        if (assigner == null || !assigner.hasWorkers()) {
            return Optional.empty();
        }
        Iterator<SplitAssignment> it = pending.iterator();
        while (it.hasNext()) {
            SplitAssignment split = it.next();
            if (!split.filePaths().isEmpty()) {
                List<String> preferred = assigner.preferredWorkers(split.filePaths(), 1);
                if (!preferred.isEmpty() && preferred.get(0).equals(workerId)) {
                    it.remove();
                    return Optional.of(split);
                }
            }
        }
        return Optional.empty();
    }

    /**
     * Polls a split whose data locations overlap with the worker's locality.
     * O(n) scan but queue is typically small.
     */
    public Optional<SplitAssignment> pollLocalSplit(List<String> workerLocality) {
        if (workerLocality == null || workerLocality.isEmpty()) {
            return Optional.empty();
        }
        Iterator<SplitAssignment> it = pending.iterator();
        while (it.hasNext()) {
            SplitAssignment split = it.next();
            if (hasOverlap(split.dataLocations(), workerLocality)) {
                it.remove();
                return Optional.of(split);
            }
        }
        return Optional.empty();
    }

    /**
     * Polls any available split (no locality preference).
     * When steal-delay is configured, only returns splits whose hold period has expired.
     * This gives the cache-affine Worker a head start before other Workers can steal.
     */
    public Optional<SplitAssignment> pollAnySplit() {
        if (stealDelayMs == 0) {
            return Optional.ofNullable(pending.poll());
        }
        long now = System.currentTimeMillis();
        Iterator<SplitAssignment> it = pending.iterator();
        while (it.hasNext()) {
            SplitAssignment split = it.next();
            if (now - split.enqueueTimeMs() >= stealDelayMs) {
                it.remove();
                return Optional.of(split);
            }
        }
        return Optional.empty();
    }

    /**
     * Polls a split with a lease using 3-tier priority:
     * 1. Cache-affine (file hashes to this worker)
     * 2. Locality-aware (data location overlap)
     * 3. Any available (load balancing fallback)
     */
    public Optional<SplitLease> pollWithLease(String workerId, List<String> workerLocality) {
        return pollWithLease(workerId, workerLocality, null);
    }

    /** Leases the next split, preferring worker-local data. */
    public Optional<SplitLease> pollWithLease(String workerId, List<String> workerLocality,
                                       CacheAffinityAssigner cacheAffinityAssigner) {
        // Priority 1: cache-affine
        Optional<SplitAssignment> split = pollCacheAffineSplit(workerId, cacheAffinityAssigner);
        // Priority 2: locality-aware
        if (split.isEmpty()) {
            split = pollLocalSplit(workerLocality);
        }
        // Priority 3: any available
        if (split.isEmpty()) {
            split = pollAnySplit();
        }
        return split.map(s -> {
            SplitLease lease = new SplitLease(
                    s.splitIndex(), workerId,
                    System.currentTimeMillis() + leaseDurationMs, s);
            activeLeases.put(s.splitIndex(), lease);
            return lease;
        });
    }

    /** Marks a split as completed and removes its lease. */
    public void markCompleted(int splitIndex) {
        activeLeases.remove(splitIndex);
        completedSplits.incrementAndGet();
    }

    /** Reclaims expired leases, returning splits to the pending queue. */
    public int reclaimExpiredLeases() {
        int reclaimed = 0;
        Iterator<java.util.Map.Entry<Integer, SplitLease>> it = activeLeases.entrySet().iterator();
        while (it.hasNext()) {
            java.util.Map.Entry<Integer, SplitLease> entry = it.next();
            if (entry.getValue().isExpired()) {
                pending.add(entry.getValue().split());
                it.remove();
                reclaimed++;
            }
        }
        return reclaimed;
    }

    public boolean allCompleted() {
        return completedSplits.get() >= totalSplits.get() && pending.isEmpty() && activeLeases.isEmpty();
    }

    public int pendingCount() {
        return pending.size();
    }

    public int activeLeaseCount() {
        return activeLeases.size();
    }

    private static boolean hasOverlap(List<String> a, List<String> b) {
        for (String s : a) {
            if (b.contains(s)) {
                return true;
            }
        }
        return false;
    }
}
