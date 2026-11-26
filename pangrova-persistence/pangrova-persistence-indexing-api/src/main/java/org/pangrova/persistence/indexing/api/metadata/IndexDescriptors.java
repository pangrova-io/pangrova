package org.pangrova.persistence.indexing.api.metadata;

import org.pangrova.common.api.annotation.Experimental;
import org.pangrova.indexing.api.descriptor.IndexDescriptor;
import org.pangrova.indexing.api.enums.IndexState;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/** Registration, source authority, descriptor transitions and tenant-scoped lookup. */
@Experimental
public interface IndexDescriptors {
    /**
     * Registers a new index descriptor. Enforces UNIQUE constraint on
     * (catalogName, schemaName, tableName, indexName).
     *
     * @throws org.pangrova.indexing.api.exception.IndexAlreadyExistsException if the index already exists
     */
    void registerIndex(IndexDescriptor descriptor);

    /** Registers the Native descriptor and caller-captured admission attempt in the same transaction. */
    default void registerNativeIndex(IndexDescriptor descriptor, String attemptId) {
        throw new UnsupportedOperationException("Durable Native submission admission is unavailable");
    }

    /** Atomically registers a scalar descriptor with its real deferred build subject. */
    default void registerIndexWithBuildAuthority(IndexDescriptor descriptor, String principal) {
        throw new UnsupportedOperationException("Durable scalar index authority is unavailable");
    }

    /** Rechecks current tenant, roles, raw column read and table alter grants before deferred access. */
    default String loadAuthorizedBuildPrincipal(IndexDescriptor descriptor) {
        throw new UnsupportedOperationException("Durable scalar index authority is unavailable");
    }

    /**
     * Loads the persisted scalar subject only for a DROPPED index's cleanup.
     * Implementations must verify the exact durable index scope and DROPPED state;
     * cleanup must remain possible after the subject's source-read grant is revoked.
     */
    default String loadDroppedIndexCleanupPrincipal(IndexDescriptor descriptor) {
        throw new UnsupportedOperationException("Durable scalar index cleanup authority is unavailable");
    }

    /**
     * Serializes external snapshot pin publication against final index cleanup.
     * The lock must survive nested metadata calls and remain held through the
     * connector mutation; implementations without a durable cross-process lock
     * must fail closed.
     */
    default void withIndexLifecycleLock(String indexId, Runnable action) {
        throw new UnsupportedOperationException("Durable index lifecycle lock is unavailable");
    }

    /**
     * Updates the state of an existing index.
     * This overload is unconditional; it must not implement a fenced operation.
     * The domain lifecycle owner validates the requested transition.
     */
    void updateIndexState(String indexId, String newState, Instant timestamp);

    /**
     * Updates the state of an existing index with a stale reason.
     * The reason is only persisted when transitioning to STALE.
     *
     * @param indexId     the index to update
     * @param newState    the target state
     * @param timestamp   the new updatedAt value
     * @param staleReason reason for staleness (persisted only for STALE transitions)
     */
    default void updateIndexState(String indexId, String newState,
                                  Instant timestamp, String staleReason) {
        throw new UnsupportedOperationException("State updates preserving stale reasons are required");
    }

    /**
     * Optimistic-locking state update. Only succeeds if the current updatedAt
     * matches {@code expectedUpdatedAt}, preventing concurrent build races.
     *
     * <p>The comparison and update must be one atomic operation. An implementation
     * without this guarantee must reject the call before any mutation; it must not
     * delegate to an unconditional update. A false result means an actual mismatch.</p>
     *
     * @throws UnsupportedOperationException if atomic timestamp fencing is unavailable
     * @return true if the update was applied, false on version mismatch
     */
    default boolean updateIndexState(String indexId, String newState,
                                     Instant timestamp, Instant expectedUpdatedAt) {
        throw new UnsupportedOperationException("Atomic timestamp-fenced state updates are required");
    }

    /**
     * Optimistic-locking state update with state+generation fencing.
     * Only succeeds if the current updatedAt, state, and generation all match
     * the expected values, preventing stale state transitions in multi-coordinator
     * deployments.
     *
     * @param indexId             the index to update
     * @param newState            the target state
     * @param timestamp           the new updatedAt value
     * @param expectedUpdatedAt   expected current updatedAt (optimistic lock)
     * @param expectedState       expected current state
     * @param expectedGeneration  expected current generation
     * @return true if the update was applied, false on mismatch
     */
    default boolean updateIndexState(String indexId, String newState,
                                     Instant timestamp, Instant expectedUpdatedAt,
                                     String expectedState, int expectedGeneration) {
        throw new UnsupportedOperationException("Atomic state and generation fencing is required");
    }

    /**
     * State+generation fenced transition that persists a STALE reason in the same
     * atomic update. Unsupported implementations must reject before any mutation;
     * dropping the reason or reducing the fence is not a supported implementation.
     */
    default boolean updateIndexState(
            String indexId,
            String newState,
            Instant timestamp,
            String staleReason,
            Instant expectedUpdatedAt,
            String expectedState,
            int expectedGeneration) {
        throw new UnsupportedOperationException("Atomic fencing with stale reason persistence is required");
    }

    /** Finds an index by its unique identifier. */
    Optional<IndexDescriptor> findIndex(String indexId);

    /** Lists indexes owned by one tenant for a given table. */
    List<IndexDescriptor> findIndexesByTable(
            String tenantId, String catalog, String schema, String table);

    /** Lists indexes across tenants for an explicitly global maintenance event. */
    default List<IndexDescriptor> findIndexesByTableForGlobalMaintenance(
            String catalog, String schema, String table) {
        return java.util.Arrays.stream(IndexState.values())
                .flatMap(state -> findIndexesByState(state).stream())
                .filter(index -> index.catalogName().equals(catalog))
                .filter(index -> index.schemaName().equals(schema))
                .filter(index -> index.tableName().equals(table))
                .toList();
    }

    /**
     * Returns READY (and optionally STALE) indexes covering at least one
     * of the specified columns.
     */
    List<IndexDescriptor> findReadyIndexesForColumns(
            String tenantId, String catalog, String schema, String table,
            List<String> columns, boolean includeStale);

    /** Marks an index as DROPPED. */
    void dropIndex(String indexId, Instant timestamp);

    /** Finds indexes covering a specific column. */
    List<IndexDescriptor> findIndexesByColumn(
            String tenantId, String catalog, String schema, String table, String column);

    /** Renames a column across all indexes for a table. */
    void renameColumnInIndexes(
            String tenantId, String catalog, String schema, String table,
            String oldColumn, String newColumn);

    /** Drops all indexes for a table. */
    void dropAllIndexesForTable(
            String tenantId, String catalog, String schema, String table, Instant timestamp);

    /** Renames a table across all its indexes. */
    void renameTable(
            String tenantId, String catalog, String schema, String oldTable, String newTable);

    /** Finds all indexes in the given state. */
    List<IndexDescriptor> findIndexesByState(IndexState state);

    /**
     * Batch-fetches index descriptors by their IDs.
     * Returns a map of indexId → IndexDescriptor for all found indexes.
     * Missing IDs are silently omitted from the result.
     */
    default Map<String, IndexDescriptor> findIndexesByIds(List<String> indexIds) {
        Map<String, IndexDescriptor> result = new java.util.HashMap<>();
        for (String id : indexIds) {
            findIndex(id).ifPresent(d -> result.put(id, d));
        }
        return result;
    }

    /** Updates the last indexed snapshot ID for an index. */
    void updateLastIndexedSnapshotId(String indexId, String snapshotId);

    /** Finds indexes by state with pagination. */
    default List<IndexDescriptor> findIndexesByStatePaginated(IndexState state, int offset, int limit) {
        if (offset < 0 || limit < 0) {
            throw new IllegalArgumentException("offset and limit must be non-negative");
        }
        return findIndexesByState(state).stream()
                .sorted(java.util.Comparator.comparing(IndexDescriptor::updatedAt)
                        .thenComparing(IndexDescriptor::indexId))
                .skip(offset).limit(limit).toList();
    }
}
