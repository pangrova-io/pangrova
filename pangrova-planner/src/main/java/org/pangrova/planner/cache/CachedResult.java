package org.pangrova.planner.cache;

import java.time.Instant;

/**
 * Immutable cached query result with timestamp.
 */
public record CachedResult(byte[] data, Instant cachedAt) {}
