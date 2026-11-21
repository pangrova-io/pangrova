package org.pangrova.persistence.indexing.api.metadata;

import org.pangrova.common.api.annotation.Experimental;
import org.pangrova.indexing.api.enums.IndexState;
import java.time.Instant;

/** Staging reservations and exact build-generation fences. */
@Experimental
public interface IndexBuildGenerations {
    /**
     * Atomically reserves a staging generation without replacing the active
     * descriptor generation. The compare-and-set fences concurrent coordinators
     * by descriptor timestamp, state, and active generation. Queries therefore
     * continue to use the active manifest while the new generation is built.
     */
    boolean beginIndexGeneration(
            String indexId,
            int newGeneration,
            String buildSnapshotId,
            Instant timestamp,
            Instant expectedUpdatedAt,
            IndexState expectedState,
            int expectedGeneration);

    /**
     * Fences and records a failed staging generation. Implementations must not
     * retire or otherwise mutate the active generation.
     *
     * @return true when the matching BUILDING generation was failed
     */
    default boolean failIndexGeneration(
            String indexId,
            int generation,
            Instant expectedBuildUpdatedAt,
            Instant failedAt,
            String failureReason) {
        throw new UnsupportedOperationException("Durable index build generation fences are required");
    }

    /** Returns whether a generation is currently fenced for build output. */
    default boolean isIndexGenerationBuildable(String indexId, int generation) {
        throw new UnsupportedOperationException("Durable index generation lookup is required");
    }

    /** Confirms that the exact snapshot is reserved by a currently building generation. */
    default boolean isIndexSnapshotBuildable(String indexId, String snapshotId) {
        throw new UnsupportedOperationException("Durable index build generation fences are required");
    }
}
