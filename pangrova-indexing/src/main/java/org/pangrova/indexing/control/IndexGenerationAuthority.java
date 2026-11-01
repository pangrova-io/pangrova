package org.pangrova.indexing.control;

import java.time.Instant;
import java.util.List;
import java.util.Optional;
import org.pangrova.indexing.api.BreakerState;
import org.pangrova.indexing.api.CacheEvent;
import org.pangrova.indexing.api.CommitLogEntry;
import org.pangrova.indexing.api.IndexGenerationManifest;
import org.pangrova.indexing.api.IndexPublicationRequest;
import org.pangrova.spi.index.IndexState;

/**
 * Generation authority of the index metadata store: staging-generation
 * fencing, publication, build claims, commit events, cache events, and
 * circuit breaker state.
 */
public interface IndexGenerationAuthority {

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
        return false;
    }

    /** Confirms that the exact snapshot is reserved by a currently building generation. */
    default boolean isIndexSnapshotBuildable(String indexId, String snapshotId) {
        return false;
    }

    /** Atomically validates and activates one complete immutable generation. */
    IndexGenerationManifest publishIndexGeneration(IndexPublicationRequest request);

    /** Finds the active immutable generation manifest for an index. */
    Optional<IndexGenerationManifest> findActiveIndexGeneration(String indexId);

    /** Attempts to claim a build for the given index+generation. Returns true if claimed. */
    default boolean claimBuild(String indexId, int generation, String ownerId, Instant now) {
        return false;
    }

    /** Releases a build claim. */
    default void releaseBuildClaim(String indexId, int generation) {}

    /** Updates heartbeat for an active build claim. */
    default void heartbeatBuildClaim(String indexId, int generation, Instant now) {}

    /** Expires build claims where heartbeat is older than threshold. Returns expired count. */
    default int expireStaleBuilds(Instant olderThan) { return 0; }

    /** Records a commit event in the shared log. */
    default void recordCommitEvent(CommitEventRequest request) {
        throw new UnsupportedOperationException(
                "Index metadata store does not support durable commit events");
    }

    /** Claims a bounded, fair batch under a durable maintenance-lease epoch. */
    default List<CommitLogEntry> pollUnprocessedCommitEvents(int limit, long leaseEpoch) {
        throw new UnsupportedOperationException(
                "Index metadata store does not support durable commit-event polling");
    }

    /** Acknowledges only the claimed head event under the same lease epoch. */
    default void markCommitEventProcessed(long eventId, long leaseEpoch) {
        throw new UnsupportedOperationException(
                "Index metadata store does not support durable commit-event acknowledgement");
    }

    /** Inserts a cache invalidation event. */
    default void insertCacheEvent(String eventType, String indexId,
                                  String catalog, String schema, String table, Instant now) {}

    /** Polls cache events after the given event ID. */
    default List<CacheEvent> pollCacheEvents(long afterEventId, int limit) { return List.of(); }

    /** Gets the circuit breaker state for an index. */
    default Optional<BreakerState> getBreakerState(String indexId) { return Optional.empty(); }

    /** Updates the circuit breaker state. Uses upsert semantics. */
    default void updateBreakerState(String indexId, int failureCount,
                                    Instant lastFailureAt, Instant lastSuccessAt, String state) {}
}
