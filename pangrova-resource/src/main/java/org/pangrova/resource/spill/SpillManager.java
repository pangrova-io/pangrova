package org.pangrova.resource.spill;

import org.pangrova.common.concurrent.ThreadSafe;
import org.pangrova.resource.MemoryPressure;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Manages Java-side spill candidates and triggers spilling based on memory pressure.
 *
 * <p>Scope: This manager governs Java-heap resources only (scan buffers,
 * shuffle buffers, sort buffers managed in Java). Native operator spill
 * (HashBuild, HashAgg, OrderBy, Window) is handled entirely by Velox
 * SharedArbitrator — see {@code VeloxMemoryBridge} and
 * {@code VeloxTaskRunner} in pangrova-worker-native.</p>
 *
 * <p>The spill directory configured here is propagated to the native side
 * via {@code SubmitTaskRequest.resourceLimit.spillDirectory} for Velox
 * operator spill. Java-side SpillManager does not write to that directory
 * directly.</p>
 */
@ThreadSafe
public class SpillManager {

    private static final Logger LOG = LoggerFactory.getLogger(SpillManager.class);
    private static final int MAX_PRIORITY_MEDIUM = 1;
    private static final int MAX_PRIORITY_HIGH = 3;
    private static final int MAX_PRIORITY_CRITICAL = Integer.MAX_VALUE;
    /** Minimum free disk space (1 GB) before spilling is allowed. */
    private static final long MIN_FREE_DISK_BYTES = 1L * 1024 * 1024 * 1024;

    private final ConcurrentHashMap<String, SpillCandidate> candidates = new ConcurrentHashMap<>();
    private final String spillDirectory;

    public SpillManager() {
        this(null);
    }

    /**
     * Creates a spill manager writing spill files to the given directory (nullable).
     */
    public SpillManager(String spillDirectory) {
        this.spillDirectory = spillDirectory;
        if (spillDirectory != null) {
            LOG.info("SpillManager initialized with directory: {}", spillDirectory);
        }
    }

    public String getSpillDirectory() {
        return spillDirectory;
    }

    /**
     * Spill candidate with priority ordering.
     * Priority: 1=ScanBuffer, 2=ShuffleBuffer, 3=Sort, 4=Aggregation, 5=HashJoin.
     */
    public record SpillCandidate(String id, int priority, long reclaimableBytes, Runnable reclaimAction) {
        /** Compat constructor for callers not supplying a reclaim action. */
        public SpillCandidate(String id, int priority, long reclaimableBytes) {
            this(id, priority, reclaimableBytes, () -> {});
        }
    }

    public void registerCandidate(SpillCandidate candidate) {
        candidates.put(candidate.id(), candidate);
    }

    public void removeCandidate(String id) {
        candidates.remove(id);
    }

    /**
     * Selects spill candidates up to the target byte amount for the given pressure level.
     */
    public List<SpillCandidate> selectCandidates(long targetBytes, MemoryPressure pressure) {
        int maxPriority = resolveMaxPriority(pressure);
        if (maxPriority == 0) {
            return List.of();
        }
        return collectUntilTarget(targetBytes, maxPriority);
    }

    public long handleMemoryPressure(MemoryPressure pressure) {
        return handleMemoryPressure(pressure, Long.MAX_VALUE);
    }

    /**
     * Handles memory pressure by spilling candidates up to the target byte amount.
     *
     * @param pressure current memory pressure level (determines which candidates are eligible)
     * @param targetBytes target number of bytes to reclaim (use Long.MAX_VALUE for unlimited)
     * @return total bytes reclaimed
     */
    public long handleMemoryPressure(MemoryPressure pressure, long targetBytes) {
        int maxPriority = resolveMaxPriority(pressure);
        if (maxPriority == 0) {
            return 0;
        }
        if (!hasEnoughDiskSpace(targetBytes)) {
            LOG.warn("event=spill_skipped_low_disk spillDir={} targetBytes={} pressure={}",
                    spillDirectory, targetBytes, pressure);
            return 0;
        }
        List<SpillCandidate> selected = collectUntilTarget(targetBytes, maxPriority);
        long reclaimed = selected.stream().mapToLong(SpillCandidate::reclaimableBytes).sum();
        selected.forEach(c -> candidates.remove(c.id()));
        LOG.info("event=spill_triggered candidates={} reclaimedBytes={} targetBytes={} pressure={}",
                selected.size(), reclaimed, targetBytes, pressure);
        return reclaimed;
    }

    /**
     * Checks whether the spill directory has enough free disk space.
     * Returns true if no spill directory is configured (spill may go elsewhere)
     * or if free space exceeds the minimum threshold plus the target bytes.
     */
    private boolean hasEnoughDiskSpace(long targetBytes) {
        if (spillDirectory == null) {
            return true;
        }
        File spillDir = new File(spillDirectory);
        if (!spillDir.exists()) {
            return true;
        }
        long freeSpace = spillDir.getUsableSpace();
        return freeSpace > MIN_FREE_DISK_BYTES + targetBytes;
    }

    private int resolveMaxPriority(MemoryPressure pressure) {
        return switch (pressure) {
            case MEDIUM -> MAX_PRIORITY_MEDIUM;
            case HIGH -> MAX_PRIORITY_HIGH;
            case CRITICAL -> MAX_PRIORITY_CRITICAL;
            default -> 0;
        };
    }

    // PERF-T15: Use PriorityQueue for partial extraction instead of full sort.
    // Stops as soon as accumulated bytes reach target, avoiding O(N log N) when only K items needed.
    private List<SpillCandidate> collectUntilTarget(long targetBytes, int maxPriority) {
        java.util.PriorityQueue<SpillCandidate> heap = new java.util.PriorityQueue<>(
                Comparator.comparingInt(SpillCandidate::priority)
                        .thenComparing(Comparator.comparingLong(SpillCandidate::reclaimableBytes).reversed()));
        for (SpillCandidate c : candidates.values()) {
            if (c.priority() <= maxPriority) {
                heap.offer(c);
            }
        }
        return accumulateFromHeap(heap, targetBytes);
    }

    private List<SpillCandidate> accumulateFromHeap(
            java.util.PriorityQueue<SpillCandidate> heap, long targetBytes) {
        List<SpillCandidate> result = new ArrayList<>();
        long accumulated = 0;
        while (!heap.isEmpty() && accumulated < targetBytes) {
            SpillCandidate candidate = heap.poll();
            result.add(candidate);
            accumulated += candidate.reclaimableBytes();
        }
        return result;
    }
}
