package org.pangrova.resource.spill;

import org.pangrova.common.concurrent.ThreadSafe;
import org.pangrova.resource.MemoryPressure;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Objects;
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
 * <p>Java candidates own their buffers and spill directories. This manager
 * inspects the configured local volume and invokes each owner synchronously.
 * Native operator spill has a separate canonical task grant and lifecycle.</p>
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
     * Creates a manager inspecting the operator-configured spill volume (nullable).
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
    public static final class SpillCandidate {
        private final SpillableBuffer buffer;

        SpillCandidate(SpillableBuffer buffer) {
            this.buffer = Objects.requireNonNull(buffer, "buffer");
        }

        public String id() {
            return buffer.spillId();
        }

        public int priority() {
            return buffer.spillPriority();
        }

        public long reclaimableBytes() {
            return buffer.reclaimableBytes();
        }

        private long reclaim(long targetBytes) {
            return buffer.spillToDisk(targetBytes);
        }
    }

    /** Registers the live buffer owner for synchronous pressure reclamation. */
    public void registerCandidate(SpillCandidate candidate) {
        Objects.requireNonNull(candidate, "candidate");
        Objects.requireNonNull(candidate.id(), "candidate.id");
        if (candidate.priority() <= 0 || candidate.reclaimableBytes() < 0) {
            throw new IllegalArgumentException("Invalid spill candidate: " + candidate.id());
        }
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
        if (maxPriority == 0 || targetBytes <= 0) {
            return 0;
        }
        long reclaimed = 0;
        int triggered = 0;
        // Selection estimates order work; only each owner's actual return counts as reclaimed.
        for (SpillCandidate candidate : collectUntilTarget(Long.MAX_VALUE, maxPriority)) {
            if (reclaimed >= targetBytes) {
                break;
            }
            long remaining = targetBytes - reclaimed;
            if (!hasEnoughDiskSpace(candidate.reclaimableBytes())) {
                LOG.warn("event=spill_skipped_low_disk spillDir={} candidate={} pressure={}",
                        spillDirectory, candidate.id(), pressure);
                continue;
            }
            long freed = candidate.reclaim(remaining);
            if (freed < 0) {
                throw new IllegalStateException("Negative actual spill bytes for " + candidate.id());
            }
            reclaimed = saturatedAdd(reclaimed, freed);
            triggered++;
            if (candidate.reclaimableBytes() == 0) {
                candidates.remove(candidate.id(), candidate);
            }
        }
        LOG.info("event=spill_triggered candidates={} reclaimedBytes={} targetBytes={} pressure={}",
                triggered, reclaimed, targetBytes, pressure);
        return reclaimed;
    }

    /** Checks the actual volume of the operator-configured directory, including missing descendants. */
    private boolean hasEnoughDiskSpace(long expectedBytes) {
        if (spillDirectory == null) {
            return true;
        }
        Path volumePath = Path.of(spillDirectory).toAbsolutePath();
        while (!Files.exists(volumePath)) {
            volumePath = volumePath.getParent();
            if (volumePath == null) {
                throw new IllegalStateException("No existing spill directory ancestor: " + spillDirectory);
            }
        }
        if (!Files.isDirectory(volumePath)) {
            throw new IllegalStateException("Spill directory is not a directory: " + volumePath);
        }
        try {
            long freeSpace = Files.getFileStore(volumePath).getUsableSpace();
            return freeSpace > MIN_FREE_DISK_BYTES
                    && expectedBytes < freeSpace - MIN_FREE_DISK_BYTES;
        } catch (IOException e) {
            throw new UncheckedIOException("Cannot inspect spill volume: " + spillDirectory, e);
        }
    }

    private static long saturatedAdd(long left, long right) {
        return right > Long.MAX_VALUE - left ? Long.MAX_VALUE : left + right;
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
            if (c.priority() <= maxPriority && c.reclaimableBytes() > 0) {
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
            accumulated = saturatedAdd(accumulated, candidate.reclaimableBytes());
        }
        return result;
    }
}
