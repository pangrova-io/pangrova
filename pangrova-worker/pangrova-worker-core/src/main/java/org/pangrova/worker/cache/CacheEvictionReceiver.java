package org.pangrova.worker.cache;

import org.pangrova.common.api.annotation.ThreadSafe;
import org.pangrova.worker.nativebridge.jni.NativeCommandBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.function.Consumer;

/**
 * Worker-side processor for cache eviction hints received from the
 * coordinator. For each file path, issues a native {@code EVICT_CACHE}
 * command and removes the path from the {@link RecentFileAccessTracker}.
 *
 * <p>The eviction is best-effort: if the native command fails (e.g. the
 * file was never cached), the failure is logged and processing continues
 * with the next path. This matches the native engine's tolerant eviction semantics.</p>
 *
 * <p>F-CHAIN-CACHE-03: cache eviction broadcast chain for snapshot
 * expiry / compaction.</p>
 */
@ThreadSafe
public final class CacheEvictionReceiver {

    private static final Logger LOG = LoggerFactory.getLogger(CacheEvictionReceiver.class);

    private final RecentFileAccessTracker tracker;
    private final Consumer<byte[]> nativeCommandSender;

    /**
     * Creates a receiver with a native command channel.
     *
     * @param tracker             the file access tracker to update (entries for
     *                            evicted files are removed so they stop appearing
     *                            in heartbeat samples)
     * @param nativeCommandSender sends a serialized NativeCommand proto to the
     *                            native engine; typically
     *                            {@code NativeRuntime::sendCommand}
     */
    public CacheEvictionReceiver(RecentFileAccessTracker tracker,
                                 Consumer<byte[]> nativeCommandSender) {
        this.tracker = tracker;
        this.nativeCommandSender = nativeCommandSender;
    }

    /**
     * Creates a receiver without native command support (Java-only eviction
     * from the access tracker). Useful for testing or when the native engine
     * is not available.
     */
    public CacheEvictionReceiver(RecentFileAccessTracker tracker) {
        this(tracker, null);
    }

    /**
     * Processes a batch of eviction hints. For each path:
     * <ol>
     *   <li>Issues {@code EVICT_CACHE} to the native engine (if sender configured)</li>
     *   <li>Removes the path from {@link RecentFileAccessTracker}</li>
     * </ol>
     *
     * @param filePaths the file paths to evict; null or empty is a no-op
     */
    public void processEvictionHints(List<String> filePaths) {
        if (filePaths == null || filePaths.isEmpty()) {
            return;
        }
        int[] counts = evictAndCount(filePaths);
        if (counts[0] > 0 || counts[1] > 0) {
            LOG.info("event=cache_eviction_processed evicted={} failed={} total={}",
                    counts[0], counts[1], filePaths.size());
        }
    }

    /** Evicts each non-blank path, returning {@code [evicted, failed]} counters. */
    private int[] evictAndCount(List<String> filePaths) {
        int evicted = 0;
        int failed = 0;
        for (String path : filePaths) {
            if (path == null || path.isBlank()) {
                continue;
            }
            if (evictSingle(path)) {
                evicted++;
            } else {
                failed++;
            }
        }
        return new int[]{evicted, failed};
    }

    /** Evicts a single path, returning false when the eviction attempt failed. */
    private boolean evictSingle(String path) {
        try {
            if (nativeCommandSender != null) {
                byte[] command = NativeCommandBuilder.evictCache(path);
                nativeCommandSender.accept(command);
            }
            tracker.remove(path);
            return true;
        } catch (Exception e) {
            LOG.warn("event=cache_eviction_failed path={} error={}",
                    path, e.getMessage());
            return false;
        }
    }
}
