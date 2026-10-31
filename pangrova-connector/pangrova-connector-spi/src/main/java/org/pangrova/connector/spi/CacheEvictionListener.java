package org.pangrova.connector.spi;

import org.pangrova.common.api.annotation.Experimental;

import java.util.Collection;

/**
 * Listener for cache eviction events produced by connector maintenance
 * operations (snapshot expiry, compaction, orphan file removal).
 *
 * <p>When a connector maintenance procedure removes or rewrites data files,
 * the native worker caches (native async data cache / SSD cache) may still
 * hold blocks from those files. Because cache keys use immutable file paths,
 * stale entries do not cause correctness issues, but they waste memory and
 * SSD space. This listener allows connectors to publish eviction hints so
 * the coordinator can broadcast them to all workers.</p>
 *
 * <p>Implementations must be thread-safe — multiple maintenance procedures
 * may fire concurrently.</p>
 *
 * <p>F-CHAIN-CACHE-03: cache eviction broadcast chain for snapshot
 * expiry / compaction.</p>
 */
@Experimental(reason = "F-CHAIN-CACHE-03: cache eviction broadcast chain")
public interface CacheEvictionListener {

    /**
     * Called when data files become unreachable due to a maintenance
     * operation (expire_snapshots, compact, remove_orphan_files).
     *
     * <p>Implementations should treat the hints as best-effort — an
     * occasional missed or duplicate hint is acceptable. The paths are
     * full storage URIs (e.g. {@code s3://bucket/table/data/00001.parquet})
     * matching the cache key space.</p>
     *
     * @param removedFilePaths the file paths that are no longer reachable;
     *                         never null, may be empty (in which case
     *                         implementations should return immediately)
     * @param reason           human-readable reason for logging
     *                         (e.g. "expire_snapshots", "compact")
     */
    void onFilesRemoved(Collection<String> removedFilePaths, String reason);

    /**
     * A no-op listener that discards all eviction events.
     * Used as the default when no broadcaster is configured.
     */
    CacheEvictionListener NOOP = (removedFilePaths, reason) -> { };
}
