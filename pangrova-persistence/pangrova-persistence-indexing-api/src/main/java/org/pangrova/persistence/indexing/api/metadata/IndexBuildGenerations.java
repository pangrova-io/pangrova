package org.pangrova.persistence.indexing.api.metadata;

import org.pangrova.common.api.annotation.Experimental;
import org.pangrova.indexing.api.descriptor.IndexDescriptor;
import org.pangrova.indexing.api.enums.IndexState;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.Set;

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

    default boolean beginIndexGeneration(
            String indexId,
            int newGeneration,
            String buildSnapshotId,
            Instant timestamp,
            Instant expectedUpdatedAt,
            IndexState expectedState,
            int expectedGeneration,
            String sourceSchemaFingerprint) {
        return beginIndexGeneration(indexId, newGeneration, buildSnapshotId, timestamp,
                expectedUpdatedAt, expectedState, expectedGeneration);
    }

    /**
     * Fences and records a failed initial or staging generation. Initial builds move
     * the exact BUILDING descriptor to STALE; online builds fail only the staging row
     * and leave the active generation untouched.
     *
     * @return true when the matching initial or staging BUILDING generation was failed
     */
    default boolean failIndexGeneration(
            String indexId,
            int generation,
            Instant expectedBuildUpdatedAt,
            Instant failedAt,
            String failureReason) {
        throw new UnsupportedOperationException("Durable index build generation fences are required");
    }

    /** Fails only the exact Native reservation attempt; a reused logical generation is not equivalent. */
    default boolean failNativeSubmissionAttempt(String indexId, int generation, String attemptId,
                                                 Instant expectedUpdatedAt, Instant failedAt, String reason) {
        throw new UnsupportedOperationException("Exact Native submission failure fence is required");
    }

    /** Returns whether a generation is currently fenced for build output. */
    default boolean isIndexGenerationBuildable(String indexId, int generation) {
        throw new UnsupportedOperationException("Durable index generation lookup is required");
    }

    /** Lists expired Native reservations that have no durable job or pin to replay. */
    default List<NativeSubmissionCandidate> listAbandonedNativeReservations(int limit, Instant cutoff) {
        throw new UnsupportedOperationException("Durable Native reservation recovery is required");
    }

    /** Rechecks one abandoned Native reservation before fencing it. */
    default Optional<NativeSubmissionCandidate> findAbandonedNativeReservation(
            String indexId, int generation, Instant cutoff) {
        throw new UnsupportedOperationException("Durable Native reservation recovery is required");
    }

    /** Finds the exact durable publication fence for a current build reservation. */
    default Optional<Instant> findIndexGenerationSnapshotReservation(
            String indexId, int generation, String snapshotId) {
        throw new UnsupportedOperationException("Exact durable index generation lookup is required");
    }

    /** Returns the persisted submission identity for an exact current reservation or descriptor generation. */
    default Optional<String> findIndexGenerationSubmissionAttempt(
            String indexId, int generation, String snapshotId) {
        throw new UnsupportedOperationException("Durable Native submission identity lookup is required");
    }

    /** Confirms the exact generation and source snapshot belong to the same reservation. */
    default boolean isIndexGenerationSnapshotBuildable(
            String indexId, int generation, String snapshotId, Instant reservationUpdatedAt) {
        throw new UnsupportedOperationException("Exact durable index generation lookup is required");
    }

    /** Confirms that the exact snapshot is reserved by a currently building generation. */
    default boolean isIndexSnapshotBuildable(String indexId, String snapshotId) {
        throw new UnsupportedOperationException("Durable index build generation fences are required");
    }

    /**
     * Lists initial BUILDING descriptors and online BUILDING or failed staging rows.
     * The claim heartbeat is included so recovery does not mistake a live attempt for
     * an abandoned one. DROPPED descriptors are never returned.
     */
    default List<IndexBuildCandidate> findIndexBuildRecoveryCandidates() {
        throw new UnsupportedOperationException("Durable index build recovery lookup is required");
    }

    /**
     * Atomically fences an abandoned initial or online generation, expires its claim,
     * and snapshots staging mappings. The cutoff is rechecked against the claim heartbeat
     * while holding the descriptor and claim locks. No physical file is deleted here.
     */
    default Optional<IndexBuildAbandonment> abandonIndexGeneration(
            String indexId,
            int generation,
            Instant expectedUpdatedAt,
            Instant cutoff,
            Instant abandonedAt,
            String reason) {
        throw new UnsupportedOperationException("Atomic fenced index build abandonment is required");
    }

    /**
     * Removes mappings and the FAILED staging row only after physical cleanup and only
     * while the exact descriptor and failed-generation fences still match. A replacement
     * attempt or DROP therefore cannot be cleaned up by an older recovery pass.
     */
    default boolean completeIndexBuildCleanup(
            String indexId,
            int generation,
            Instant expectedUpdatedAt,
            Instant expectedBuildUpdatedAt,
            IndexState expectedState,
            int expectedGeneration) {
        throw new UnsupportedOperationException("Fenced index build cleanup is required");
    }

    /** Exact Native reservation candidate; the attempt survives equal timestamps and generation reuse. */
    record NativeSubmissionCandidate(IndexDescriptor descriptor, int generation, Instant updatedAt, String attemptId) {
        public NativeSubmissionCandidate {
            if (descriptor == null || generation < 0 || updatedAt == null || attemptId == null || attemptId.isBlank()) {
                throw new IllegalArgumentException("A complete Native submission candidate is required");
            }
        }
    }

    /** One durable recovery candidate; initial builds have no separate staging row. */
    record IndexBuildCandidate(
            IndexDescriptor descriptor,
            int generation,
            String state,
            Instant updatedAt,
            Instant claimHeartbeatAt) {

        public IndexBuildCandidate {
            if (descriptor == null || generation < 0 || state == null || updatedAt == null) {
                throw new IllegalArgumentException("A complete index build recovery candidate is required");
            }
        }

        public Instant lastActivityAt() {
            return claimHeartbeatAt == null || claimHeartbeatAt.isBefore(updatedAt)
                    ? updatedAt : claimHeartbeatAt;
        }
    }

    /** Committed abandonment evidence; paths exclude objects still referenced by another generation. */
    record IndexBuildAbandonment(
            IndexDescriptor descriptor,
            List<IndexFileMapping> mappings,
            Set<String> pathsToDelete) {

        public IndexBuildAbandonment {
            descriptor = java.util.Objects.requireNonNull(descriptor, "descriptor");
            mappings = List.copyOf(mappings);
            pathsToDelete = Set.copyOf(pathsToDelete);
        }
    }
}
