package org.pangrova.indexing.descriptor;

import org.pangrova.indexing.api.descriptor.IndexDescriptor;
import org.pangrova.indexing.api.enums.IndexState;
import java.time.Instant;
import java.util.Map;
import java.util.Set;

/** Pure descriptor transitions; durable publication still requires the fenced repository operation. */
public final class IndexDescriptorTransitions {
    private IndexDescriptorTransitions() {}

    private static final Map<IndexState, Set<IndexState>> ALLOWED_TRANSITIONS = Map.of(
            IndexState.BUILDING, Set.of(IndexState.BUILDING, IndexState.READY, IndexState.STALE, IndexState.DROPPED),
            // READY → BUILDING is allowed so users can request a rebuild without first
            // marking the index STALE (e.g. forced refresh after schema/property change).
            IndexState.READY, Set.of(IndexState.BUILDING, IndexState.STALE, IndexState.DROPPED),
            IndexState.STALE, Set.of(IndexState.BUILDING, IndexState.DROPPED),
            IndexState.DROPPED, Set.of()
    );

    /**
     * Transitions this descriptor to a new state, enforcing the state machine.
     *
     * <p>Clears {@code staleReason} when transitioning to a non-STALE state.</p>
     *
     * @param newState  target state
     * @param timestamp update timestamp
     * @return new descriptor with updated state and timestamp
     * @throws IllegalStateException if the transition is not allowed
     */
    public static IndexDescriptor transition(IndexDescriptor descriptor, IndexState newState, Instant timestamp) {
        validateTransition(descriptor.state(), newState);
        String reason = newState == IndexState.STALE ? descriptor.staleReason() : null;
        return new IndexDescriptor(
                descriptor.indexId(), descriptor.indexName(), descriptor.catalogName(),
                descriptor.schemaName(), descriptor.tableName(),
                descriptor.columnNames(), descriptor.indexType(), descriptor.properties(),
                newState, reason, descriptor.generation(), descriptor.buildSnapshotId(),
                descriptor.lastIndexedSnapshotId(),
                descriptor.createdAt(), timestamp, descriptor.tenantId());
    }

    /**
     * Transitions this descriptor to a new state with an explicit stale reason.
     *
     * <p>The {@code staleReason} is applied only when transitioning to
     * {@link IndexState#STALE}; for other target states it is ignored and
     * cleared.</p>
     *
     * @param newState    target state
     * @param reasonWhenStale reason for staleness
     * @param timestamp   update timestamp
     * @return new descriptor with updated state, reason, and timestamp
     * @throws IllegalStateException if the transition is not allowed
     */
    public static IndexDescriptor transition(
            IndexDescriptor descriptor, IndexState newState, String reasonWhenStale, Instant timestamp) {
        validateTransition(descriptor.state(), newState);
        String reason = newState == IndexState.STALE ? reasonWhenStale : null;
        return new IndexDescriptor(
                descriptor.indexId(), descriptor.indexName(), descriptor.catalogName(),
                descriptor.schemaName(), descriptor.tableName(),
                descriptor.columnNames(), descriptor.indexType(), descriptor.properties(),
                newState, reason, descriptor.generation(), descriptor.buildSnapshotId(),
                descriptor.lastIndexedSnapshotId(),
                descriptor.createdAt(), timestamp, descriptor.tenantId());
    }

    private static void validateTransition(IndexState state, IndexState newState) {
        Set<IndexState> allowed = ALLOWED_TRANSITIONS.get(state);
        if (allowed == null || !allowed.contains(newState)) {
            throw new IllegalStateException(
                    "Invalid state transition from " + state + " to " + newState);
        }
    }
}
