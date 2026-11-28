package org.pangrova.coordinator.scheduler.placement;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.pangrova.common.concurrency.ThreadSafe;
import org.pangrova.coordinator.worker.TopologyListener;
import org.pangrova.coordinator.worker.WorkerInfo;

/**
 * Consistent hash ring for file→Worker affinity routing.
 * Maps data file paths to preferred Worker nodes using consistent hashing,
 * maximizing NVMe cache hit rates for repeated workloads.
 *
 * <p>Thread-safe: uses {@link ReadWriteLock} for concurrent read access
 * with exclusive write access during topology updates.</p>
 *
 * <p>Implements {@link TopologyListener} to automatically rebuild the ring
 * when workers join or leave the cluster.</p>
 */
@ThreadSafe
public final class CacheAffinityAssigner implements TopologyListener {

    private static final int VIRTUAL_NODES_PER_WORKER = 128;
    private static final long HASH_SEED = 0x9747b28cL;
    private static final int BYTE_HASH_MULTIPLIER = 31;
    private static final int FMIX_SHIFT = 33;
    private static final long FMIX_MULTIPLIER_ONE = 0xff51afd7ed558ccdL;
    private static final long FMIX_MULTIPLIER_TWO = 0xc4ceb9fe1a85ec53L;

    private final TreeMap<Long, String> ring = new TreeMap<>();
    private final ReadWriteLock lock = new ReentrantReadWriteLock();

    /**
     * Rebuilds the hash ring when worker topology changes (join/leave/heartbeat timeout).
     */
    @Override
    public void onTopologyChanged(List<WorkerInfo> activeWorkers) {
        updateTopology(activeWorkers);
    }

    /**
     * Rebuilds the hash ring with the given worker list.
     */
    public void updateTopology(List<WorkerInfo> activeWorkers) {
        lock.writeLock().lock();
        try {
            ring.clear();
            if (activeWorkers == null) return;
            for (WorkerInfo w : activeWorkers) {
                for (int v = 0; v < VIRTUAL_NODES_PER_WORKER; v++) {
                    long hash = murmurHash(w.workerId() + "#" + v);
                    ring.put(hash, w.workerId());
                }
            }
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Returns the preferred Worker for a given data file path.
     * The same file always maps to the same Worker as long as topology is stable.
     */
    public String preferredWorker(String filePath) {
        if (filePath == null) return null;
        long hash = murmurHash(filePath);
        lock.readLock().lock();
        try {
            if (ring.isEmpty()) return null;
            Map.Entry<Long, String> entry = ring.ceilingEntry(hash);
            if (entry == null) entry = ring.firstEntry();
            return entry != null ? entry.getValue() : null;
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * Returns an ordered list of preferred Workers for a split with multiple files.
     * Workers are ranked by how many of the split's files hash to them.
     */
    public List<String> preferredWorkers(List<String> filePaths, int maxCandidates) {
        if (filePaths == null || filePaths.isEmpty() || maxCandidates <= 0) {
            return List.of();
        }
        Map<String, Integer> workerHits = new HashMap<>();
        for (String path : filePaths) {
            String w = preferredWorker(path);
            if (w != null) {
                workerHits.merge(w, 1, Integer::sum);
            }
        }
        return workerHits.entrySet().stream()
                .sorted(Map.Entry.<String, Integer>comparingByValue().reversed())
                .limit(maxCandidates)
                .map(Map.Entry::getKey)
                .toList();
    }

    /** Returns true if the ring has any workers. */
    public boolean hasWorkers() {
        lock.readLock().lock();
        try {
            return !ring.isEmpty();
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * Polynomial UTF-8 byte hash with the MurmurHash3 fmix64 finalization.
     */
    static long murmurHash(String key) {
        byte[] data = key.getBytes(StandardCharsets.UTF_8);
        long h = HASH_SEED;
        for (byte b : data) {
            h = h * BYTE_HASH_MULTIPLIER + b;
        }
        h ^= h >>> FMIX_SHIFT;
        h *= FMIX_MULTIPLIER_ONE;
        h ^= h >>> FMIX_SHIFT;
        h *= FMIX_MULTIPLIER_TWO;
        h ^= h >>> FMIX_SHIFT;
        return h;
    }
}
