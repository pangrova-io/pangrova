package org.pangrova.coordinator.cache;

import org.pangrova.spi.annotation.ThreadSafe;
import org.pangrova.spi.connector.CacheEvictionListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;

/**
 * Coordinator-side aggregator that collects cache eviction hints from
 * connector maintenance procedures and distributes them to all workers.
 *
 * <p>Implements {@link CacheEvictionListener} so connectors can publish
 * eviction events without depending on coordinator internals. Hints are
 * accumulated in a bounded buffer and drained by worker heartbeat responses
 * or a dedicated distribution tick.</p>
 *
 * <p>Thread-safety: all public methods are synchronized on the internal
 * buffer. The buffer is bounded to {@link #DEFAULT_MAX_PENDING} entries
 * to prevent unbounded growth from large maintenance operations.</p>
 *
 * <p>F-CHAIN-CACHE-03: cache eviction broadcast chain for snapshot
 * expiry / compaction.</p>
 */
@ThreadSafe
public final class CacheEvictionBroadcaster implements CacheEvictionListener {

    private static final Logger LOG = LoggerFactory.getLogger(CacheEvictionBroadcaster.class);

    /** Maximum pending eviction hints before oldest entries are dropped. */
    public static final int DEFAULT_MAX_PENDING = 10_000;

    private final int maxPending;
    private final LinkedHashSet<String> pendingPaths;

    public CacheEvictionBroadcaster() {
        this(DEFAULT_MAX_PENDING);
    }

    /** Creates a broadcaster with a bounded pending notification queue. */
    public CacheEvictionBroadcaster(int maxPending) {
        if (maxPending <= 0) {
            throw new IllegalArgumentException("maxPending must be > 0, got: " + maxPending);
        }
        this.maxPending = maxPending;
        this.pendingPaths = new LinkedHashSet<>();
    }

    /**
     * Records eviction hints from a connector maintenance operation.
     * Oldest entries are silently dropped when the buffer is full.
     */
    @Override
    public void onFilesRemoved(Collection<String> removedFilePaths, String reason) {
        if (removedFilePaths == null || removedFilePaths.isEmpty()) {
            return;
        }
        synchronized (pendingPaths) {
            for (String path : removedFilePaths) {
                if (path != null && !path.isBlank()) {
                    pendingPaths.add(path);
                }
            }
            // Trim oldest entries if over capacity
            while (pendingPaths.size() > maxPending) {
                var it = pendingPaths.iterator();
                it.next();
                it.remove();
            }
        }
        LOG.info("event=cache_eviction_hints_received reason={} pathCount={} pendingTotal={}",
                reason, removedFilePaths.size(), pendingSize());
    }

    /**
     * Drains up to {@code maxBatch} pending eviction hints for distribution
     * to workers. The returned paths are removed from the pending buffer.
     *
     * @param maxBatch maximum number of paths to return in one batch
     * @return an immutable list of file paths to evict; empty if none pending
     */
    public List<String> drainBatch(int maxBatch) {
        if (maxBatch <= 0) {
            return List.of();
        }
        synchronized (pendingPaths) {
            if (pendingPaths.isEmpty()) {
                return List.of();
            }
            List<String> batch = new ArrayList<>(Math.min(maxBatch, pendingPaths.size()));
            var it = pendingPaths.iterator();
            while (it.hasNext() && batch.size() < maxBatch) {
                batch.add(it.next());
                it.remove();
            }
            return List.copyOf(batch);
        }
    }

    /**
     * Returns all pending paths without removing them. For testing/monitoring.
     */
    public List<String> peekAll() {
        synchronized (pendingPaths) {
            return List.copyOf(pendingPaths);
        }
    }

    /**
     * Returns the number of pending eviction hints.
     */
    public int pendingSize() {
        synchronized (pendingPaths) {
            return pendingPaths.size();
        }
    }

    /**
     * Clears all pending eviction hints.
     */
    public void clear() {
        synchronized (pendingPaths) {
            pendingPaths.clear();
        }
    }
}
