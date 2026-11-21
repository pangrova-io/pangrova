package org.pangrova.worker.cache;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.pangrova.common.concurrent.ThreadSafe;

/**
 * Bounded LRU tracker of recently-accessed file paths on a Worker.
 *
 * <p>F-CHAIN-CACHE-02 producer: feeds the heartbeat
 * {@link org.pangrova.protocol.rest.worker.HeartbeatRequest#cachedFilePaths()}
 * field so the coordinator scheduler can route splits to workers that have
 * already touched the corresponding files. Tracks every file path the worker
 * is asked to process via task submission; the heuristic is "this worker
 * recently saw this file, so cache state may be warm".</p>
 */
@ThreadSafe
public final class RecentFileAccessTracker {

    public static final int DEFAULT_CAPACITY = 1024;
    public static final int DEFAULT_HEARTBEAT_SAMPLE_SIZE = 100;

    private final int capacity;
    private final LinkedHashMap<String, Boolean> recent;

    /** Standard hash table load factor. */
    private static final float LOAD_FACTOR = 0.75f;

    public RecentFileAccessTracker() {
        this(DEFAULT_CAPACITY);
    }

    /** Creates a tracker with the given capacity. */
    public RecentFileAccessTracker(int capacity) {
        if (capacity <= 0) {
            throw new IllegalArgumentException("capacity must be > 0, got: " + capacity);
        }
        this.capacity = capacity;
        this.recent = new LinkedHashMap<>(Math.min(capacity, 256), LOAD_FACTOR, true) {
            @Override
            protected boolean removeEldestEntry(Map.Entry<String, Boolean> eldest) {
                return super.size() > capacity;
            }
        };
    }

    /** Records access of a single file path. */
    public void recordAccess(String filePath) {
        if (filePath == null || filePath.isBlank()) {
            return;
        }
        synchronized (recent) {
            recent.put(filePath, Boolean.TRUE);
        }
    }

    /** Records access of multiple file paths. */
    public void recordAccess(List<String> filePaths) {
        if (filePaths == null || filePaths.isEmpty()) {
            return;
        }
        synchronized (recent) {
            for (String path : filePaths) {
                if (path != null && !path.isBlank()) {
                    recent.put(path, Boolean.TRUE);
                }
            }
        }
    }

    /** Returns up to {@code maxPaths} recently accessed paths. */
    public List<String> recentSample(int maxPaths) {
        if (maxPaths <= 0) {
            return List.of();
        }
        synchronized (recent) {
            int size = recent.size();
            if (size == 0) {
                return List.of();
            }
            ArrayList<String> all = new ArrayList<>(recent.keySet());
            int from = Math.max(0, all.size() - maxPaths);
            return List.copyOf(all.subList(from, all.size()));
        }
    }

    public List<String> heartbeatSample() {
        return recentSample(DEFAULT_HEARTBEAT_SAMPLE_SIZE);
    }

    public int size() {
        synchronized (recent) {
            return recent.size();
        }
    }

    public int capacity() {
        return capacity;
    }

    /** Returns whether the given path was recently accessed. */
    public boolean contains(String filePath) {
        if (filePath == null) {
            return false;
        }
        synchronized (recent) {
            return recent.containsKey(filePath);
        }
    }

    /**
     * Removes a specific file path from tracking (e.g. after cache eviction).
     */
    public void remove(String filePath) {
        if (filePath == null || filePath.isEmpty()) {
            return;
        }
        synchronized (recent) {
            recent.remove(filePath);
        }
    }

    public void clear() {
        synchronized (recent) {
            recent.clear();
        }
    }

    @Override
    public String toString() {
        synchronized (recent) {
            return "RecentFileAccessTracker{capacity=" + capacity + ", size=" + recent.size() + "}";
        }
    }

    List<String> snapshotForTesting() {
        synchronized (recent) {
            return Collections.unmodifiableList(new ArrayList<>(recent.keySet()));
        }
    }
}
