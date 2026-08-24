package org.pangrova.worker.cache;

import static org.pangrova.common.util.Preconditions.checkArgument;
import static org.pangrova.common.util.Preconditions.checkNotNull;

/**
 * Java-side configuration for the native async data cache and SSD cache.
 * Passed across the JNI boundary during native engine initialization.
 *
 * @param enabled                whether native caching is active
 * @param memoryCacheBytes       RAM budget for the in-memory data cache (e.g. 8 GB)
 * @param ssdCachePath           local path for SSD-backed cache (e.g. /mnt/nvme0/native-cache)
 * @param ssdCacheBytes          SSD cache capacity in bytes (e.g. 100 GB)
 * @param ssdShards              number of SSD cache shards for parallel I/O (e.g. 8)
 * @param checkpointIntervalBytes SSD checkpoint interval in bytes; 0 disables checkpointing
 *                                (native SsdCache won't persist its index, so restarts lose
 *                                the warm cache). Set to e.g. 1 GB for warm restart.
 */
public record NativeCacheConfig(
        boolean enabled,
        long memoryCacheBytes,
        String ssdCachePath,
        long ssdCacheBytes,
        int ssdShards,
        long checkpointIntervalBytes
) {

    public NativeCacheConfig {
        checkNotNull(ssdCachePath, "ssdCachePath must not be null");
        if (enabled) {
            checkArgument(memoryCacheBytes > 0, "memoryCacheBytes must be positive when enabled");
            checkArgument(ssdCacheBytes >= 0, "ssdCacheBytes must be non-negative");
            checkArgument(ssdShards > 0, "ssdShards must be positive when enabled");
            checkArgument(checkpointIntervalBytes >= 0,
                    "checkpointIntervalBytes must be non-negative");
        }
    }

    /**
     * Convenience constructor for backward compatibility (no checkpoint interval).
     */
    public NativeCacheConfig(boolean enabled, long memoryCacheBytes,
                             String ssdCachePath, long ssdCacheBytes, int ssdShards) {
        this(enabled, memoryCacheBytes, ssdCachePath, ssdCacheBytes, ssdShards, 0);
    }

    /** Disabled sentinel — no native caching. */
    public static final NativeCacheConfig DISABLED =
            new NativeCacheConfig(false, 0, "/dev/null", 0, 1, 0);

    /** Default checkpoint interval: 1 GB — enables SSD warm restart. */
    public static final long DEFAULT_CHECKPOINT_INTERVAL_BYTES = 1L * 1024 * 1024 * 1024;

    /** Default config: 8 GB memory cache, 100 GB SSD cache with 8 shards, 1 GB checkpoint. */
    public static final NativeCacheConfig DEFAULT = new NativeCacheConfig(
            true,
            8L * 1024 * 1024 * 1024,
            "/mnt/nvme0/native-cache",
            100L * 1024 * 1024 * 1024,
            8,
            DEFAULT_CHECKPOINT_INTERVAL_BYTES);
}
