package org.pangrova.coordinator.indexing.lifecycle.gc;

/**
 * Result of a single garbage collection run.
 *
 * @param orphanedCount number of orphaned index file mappings cleaned
 * @param purgedCount   number of DROPPED indexes fully purged
 */
public record GcResult(int orphanedCount, int purgedCount) {

    public GcResult {
        if (orphanedCount < 0) throw new IllegalArgumentException("orphanedCount must be >= 0");
        if (purgedCount < 0) throw new IllegalArgumentException("purgedCount must be >= 0");
    }

    public static final GcResult EMPTY = new GcResult(0, 0);
}
