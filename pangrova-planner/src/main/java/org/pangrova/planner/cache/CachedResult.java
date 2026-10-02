package org.pangrova.planner.cache;

import java.time.Instant;

/**
 * Immutable cached query result with timestamp.
 */
public record CachedResult(byte[] data, Instant cachedAt) {
    /** Owns cached bytes independently of producers and callers. */
    public CachedResult {
        data = data.clone();
    }

    @Override
    public byte[] data() {
        return data.clone();
    }
}
