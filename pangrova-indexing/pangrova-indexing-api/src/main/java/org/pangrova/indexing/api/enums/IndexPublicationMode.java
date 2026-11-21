package org.pangrova.indexing.api.enums;

import org.pangrova.common.api.annotation.Experimental;

import java.time.Instant;

/** Descriptor conditions for initial publication and online replacement of an index generation. */
@Experimental
public enum IndexPublicationMode {
    INITIAL_BUILD,
    ONLINE_REPLACEMENT;

    public static boolean matchesInitialBuild(IndexState state, int currentGeneration, Instant updatedAt,
            int requestedGeneration, Instant expectedUpdatedAt) {
        return state == IndexState.BUILDING && currentGeneration == requestedGeneration
                && updatedAt.equals(expectedUpdatedAt);
    }

    /** The caller must also lock and validate the staging build before activating the replacement. */
    public static boolean isReplacementCandidate(IndexState state, int currentGeneration, int requestedGeneration) {
        return (state == IndexState.READY || state == IndexState.STALE)
                && (long) currentGeneration + 1 == requestedGeneration;
    }
}
