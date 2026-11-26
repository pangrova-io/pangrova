package org.pangrova.persistence.indexing.api.build;

import java.time.Instant;
import java.util.List;
import org.pangrova.common.api.annotation.Experimental;

/**
 * Durable pre-write tracking for scalar objects not yet transferred into file mappings.
 * New physical writes must fail closed when the configured backend lacks this authority.
 * The writer marks a successful synchronous close as drained even after its claim is lost.
 * An uncertain or unconfirmed write is never eligible for automatic deletion.
 */
@Experimental
public interface ScalarArtifactIntents {
    default void beginScalarArtifactWrite(ScalarArtifactIntent intent) {
        throw new UnsupportedOperationException("Durable scalar artifact tracking is required");
    }

    default void markScalarArtifactDrained(ScalarArtifactIntent intent) {
        throw new UnsupportedOperationException("Durable scalar artifact tracking is required");
    }

    /** Bounded selection; candidates must be rechecked before external deletion. */
    default List<ScalarArtifactIntent> listReclaimableScalarArtifacts(Instant drainedBefore, int limit) {
        return List.of();
    }

    /** Selects a bounded fair batch and durably advances its scheduling priority before external work. */
    default List<ScalarArtifactIntent> pollScalarArtifactReclaimCandidates(Instant drainedBefore, int limit) {
        return listReclaimableScalarArtifacts(drainedBefore, limit);
    }

    /** Atomically fences mapping transfer and confirms no exact writer claim or mapping reference exists. */
    default boolean beginScalarArtifactReclaim(ScalarArtifactIntent intent) {
        throw new UnsupportedOperationException("Durable scalar artifact reclamation is required");
    }

    /** Retires the exact pending intent only after external deletion is confirmed. */
    default void completeScalarArtifactReclaim(ScalarArtifactIntent intent) {
        throw new UnsupportedOperationException("Durable scalar artifact reclamation is required");
    }

    /** True while any pre-mapping write or deletion still requires durable ownership. */
    default boolean hasScalarArtifactIntents(String indexId) {
        return false;
    }
}
