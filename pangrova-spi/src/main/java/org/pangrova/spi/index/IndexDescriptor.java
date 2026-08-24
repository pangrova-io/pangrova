package org.pangrova.spi.index;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Immutable descriptor for an index, carrying all metadata needed to
 * identify, build, evaluate, and manage the index lifecycle.
 *
 * <p>State transitions follow a strict state machine enforced by
 * {@link #withState(IndexState, Instant)}. See {@link IndexState} for
 * the transition diagram.</p>
 *
 * <p>Collections ({@code columnNames}, {@code properties}) are defensively
 * copied on construction to guarantee immutability.</p>
 *
 * @param indexId                unique index identifier
 * @param indexName              human-readable index name
 * @param catalogName            catalog containing the indexed table
 * @param schemaName             schema containing the indexed table
 * @param tableName              indexed table name
 * @param columnNames            columns covered by this index (non-empty)
 * @param indexType              the index strategy type
 * @param properties             index-specific configuration properties
 * @param state                  current lifecycle state
 * @param staleReason            reason for STALE state; {@code null} when not STALE
 * @param generation             rebuild generation counter (default 0)
 * @param buildSnapshotId        snapshot at which this generation was built; {@code null} if unknown
 * @param lastIndexedSnapshotId  last snapshot processed; {@code null} if unknown
 * @param createdAt              creation timestamp
 * @param updatedAt              last update timestamp
 * @param tenantId               owning tenant identifier; defaults to {@code "default"} if null
 */
public record IndexDescriptor(
        String indexId,
        String indexName,
        String catalogName,
        String schemaName,
        String tableName,
        List<String> columnNames,
        IndexType indexType,
        Map<String, String> properties,
        IndexState state,
        String staleReason,
        int generation,
        String buildSnapshotId,
        String lastIndexedSnapshotId,
        Instant createdAt,
        Instant updatedAt,
        String tenantId) {

    private static final Map<IndexState, Set<IndexState>> ALLOWED_TRANSITIONS = Map.of(
            IndexState.BUILDING, Set.of(IndexState.BUILDING, IndexState.READY, IndexState.STALE, IndexState.DROPPED),
            // READY → BUILDING is allowed so users can request a rebuild without first
            // marking the index STALE (e.g. forced refresh after schema/property change).
            IndexState.READY, Set.of(IndexState.BUILDING, IndexState.STALE, IndexState.DROPPED),
            IndexState.STALE, Set.of(IndexState.BUILDING, IndexState.DROPPED),
            IndexState.DROPPED, Set.of()
    );

    /**
     * Compact constructor — validates required fields and defensively copies collections.
     */
    public IndexDescriptor {
        requireText(indexId, "indexId");
        requireText(indexName, "indexName");
        requireText(catalogName, "catalogName");
        requireText(schemaName, "schemaName");
        requireText(tableName, "tableName");
        requireNonNull(indexType, "indexType");
        requireNonNull(state, "state");
        if (columnNames == null || columnNames.isEmpty()) {
            throw new IllegalArgumentException("columnNames must not be null or empty");
        }
        columnNames = List.copyOf(columnNames);
        properties = properties == null ? Map.of() : Map.copyOf(properties);
        tenantId = tenantId == null ? "default" : tenantId;
    }

    private static void requireText(String value, String fieldName) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(fieldName + " must not be null or blank");
        }
    }

    private static void requireNonNull(Object value, String fieldName) {
        if (value == null) {
            throw new IllegalArgumentException(fieldName + " must not be null");
        }
    }

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
    public IndexDescriptor withState(IndexState newState, Instant timestamp) {
        validateTransition(newState);
        String reason = newState == IndexState.STALE ? this.staleReason : null;
        return new IndexDescriptor(
                indexId, indexName, catalogName, schemaName, tableName,
                columnNames, indexType, properties,
                newState, reason, generation, buildSnapshotId, lastIndexedSnapshotId,
                createdAt, timestamp, tenantId);
    }

    /**
     * Transitions this descriptor to a new state with an explicit stale reason.
     *
     * <p>The {@code staleReason} is applied only when transitioning to
     * {@link IndexState#STALE}; for other target states it is ignored and
     * cleared.</p>
     *
     * @param newState    target state
     * @param staleReason reason for staleness
     * @param timestamp   update timestamp
     * @return new descriptor with updated state, reason, and timestamp
     * @throws IllegalStateException if the transition is not allowed
     */
    public IndexDescriptor withState(IndexState newState, String reasonWhenStale, Instant timestamp) {
        validateTransition(newState);
        String reason = newState == IndexState.STALE ? reasonWhenStale : null;
        return new IndexDescriptor(
                indexId, indexName, catalogName, schemaName, tableName,
                columnNames, indexType, properties,
                newState, reason, generation, buildSnapshotId, lastIndexedSnapshotId,
                createdAt, timestamp, tenantId);
    }

    /**
     * Returns a new descriptor with an updated generation counter.
     *
     * @param newGeneration the new generation value
     * @param timestamp     update timestamp
     * @return new descriptor with updated generation and timestamp
     */
    public IndexDescriptor withGeneration(int newGeneration, Instant timestamp) {
        return new IndexDescriptor(
                indexId, indexName, catalogName, schemaName, tableName,
                columnNames, indexType, properties,
                state, staleReason, newGeneration, buildSnapshotId, lastIndexedSnapshotId,
                createdAt, timestamp, tenantId);
    }

    /**
     * Returns a new descriptor with an updated build snapshot identifier.
     *
     * @param newBuildSnapshotId the snapshot at which this generation was built
     * @param timestamp          update timestamp
     * @return new descriptor with updated buildSnapshotId and timestamp
     */
    public IndexDescriptor withBuildSnapshotId(String newBuildSnapshotId, Instant timestamp) {
        return new IndexDescriptor(
                indexId, indexName, catalogName, schemaName, tableName,
                columnNames, indexType, properties,
                state, staleReason, generation, newBuildSnapshotId, lastIndexedSnapshotId,
                createdAt, timestamp, tenantId);
    }

    private void validateTransition(IndexState newState) {
        Set<IndexState> allowed = ALLOWED_TRANSITIONS.get(state);
        if (allowed == null || !allowed.contains(newState)) {
            throw new IllegalStateException(
                    "Invalid state transition from " + state + " to " + newState);
        }
    }
}
