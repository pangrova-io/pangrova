package org.pangrova.indexing.control;

import java.time.Instant;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.pangrova.indexing.api.IndexAlreadyExistsException;
import org.pangrova.spi.index.IndexDescriptor;
import org.pangrova.spi.index.IndexState;

/**
 * Descriptor authority of the index metadata store: index CRUD, state
 * transitions, schema evolution, and snapshot tracking.
 */
public interface IndexDescriptorAuthority {

    /**
     * Registers a new index descriptor. Enforces UNIQUE constraint on
     * (catalogName, schemaName, tableName, indexName).
     *
     * @throws IndexAlreadyExistsException if the index already exists
     */
    void registerIndex(IndexDescriptor descriptor);

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
     * Validates state transition via {@link IndexDescriptor#withState}.
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
        updateIndexState(indexId, newState, timestamp);
    }

    /**
     * Optimistic-locking state update. Only succeeds if the current updatedAt
     * matches {@code expectedUpdatedAt}, preventing concurrent build races.
     *
     * @return true if the update was applied, false on version mismatch
     */
    default boolean updateIndexState(String indexId, String newState,
                                     Instant timestamp, Instant expectedUpdatedAt) {
        updateIndexState(indexId, newState, timestamp);
        return true;
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
        return updateIndexState(indexId, newState, timestamp, expectedUpdatedAt);
    }

    /** State+generation fenced transition that also persists a STALE reason. */
    default boolean updateIndexState(
            String indexId,
            String newState,
            Instant timestamp,
            String staleReason,
            Instant expectedUpdatedAt,
            String expectedState,
            int expectedGeneration) {
        return updateIndexState(
                indexId, newState, timestamp, expectedUpdatedAt,
                expectedState, expectedGeneration);
    }

    /** Finds an index by its unique identifier. */
    Optional<IndexDescriptor> findIndex(String indexId);

    /** Lists indexes owned by one tenant for a given table. */
    List<IndexDescriptor> findIndexesByTable(
            String tenantId, String catalog, String schema, String table);

    /** Lists indexes across tenants for an explicitly global maintenance event. */
    default List<IndexDescriptor> findIndexesByTableForGlobalMaintenance(
            String catalog, String schema, String table) {
        return Arrays.stream(IndexState.values())
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

    /** Permanently deletes descriptor and all file mappings (for GC after grace period). */
    void purgeIndex(String indexId);

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
        Map<String, IndexDescriptor> result = new HashMap<>();
        for (String id : indexIds) {
            findIndex(id).ifPresent(d -> result.put(id, d));
        }
        return result;
    }

    /** Finds indexes by state with pagination. */
    default List<IndexDescriptor> findIndexesByStatePaginated(IndexState state, int offset, int limit) {
        return findIndexesByState(state);
    }

    /** Updates the last indexed snapshot ID for an index. */
    void updateLastIndexedSnapshotId(String indexId, String snapshotId);
}
