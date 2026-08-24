package org.pangrova.spi.coordination;

import java.util.List;

/**
 * Compact summary of a worker's data cache state, reported via heartbeat.
 *
 * <p>F-CHAIN-CACHE-02: carries a bounded sample of recently-accessed file
 * paths so that the coordinator scheduler can route splits to workers that
 * have already cached the corresponding files. Bounded at the producer side
 * (max ~100 paths). Path comparison is exact — no false positives. Replaces
 * the dead bloom-filter approach (producer-empty + transport-dropped +
 * consumer-never-called).</p>
 */
public record CacheCatalogSummary(
        List<String> cachedFilePaths,
        long cacheUsedBytes,
        long cacheMaxBytes,
        long cacheHitCount,
        long cacheMissCount
) {

    public CacheCatalogSummary {
        cachedFilePaths = cachedFilePaths == null ? List.of() : List.copyOf(cachedFilePaths);
    }

    public static final CacheCatalogSummary EMPTY = new CacheCatalogSummary(List.of(), 0, 0, 0, 0);

    /** Returns whether the cache summary contains the exact file path. */
    public boolean mightContain(String filePath) {
        if (filePath == null || cachedFilePaths.isEmpty()) {
            return false;
        }
        return cachedFilePaths.contains(filePath);
    }

    public boolean isEmpty() {
        return cachedFilePaths.isEmpty();
    }
}
