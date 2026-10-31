package org.pangrova.indexing.store;

import org.pangrova.metadata.exception.IndexAlreadyExistsException;
import org.pangrova.indexing.api.descriptor.IndexDescriptor;
import org.pangrova.indexing.api.state.IndexState;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Persistent store for index metadata: descriptors and file mappings.
 *
 * <p>Provides CRUD operations for {@link IndexDescriptor} lifecycle management
 * and {@link IndexFileMapping} tracking for build, GC, and evaluation.</p>
 */
public interface IndexMetadataStore {

    // --- IndexDescriptor CRUD ---

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

    /** Returns whether a generation is currently fenced for build output. */
    default boolean isIndexGenerationBuildable(String indexId, int generation) {
        return findIndex(indexId)
                .filter(descriptor -> descriptor.state() == IndexState.BUILDING)
                .filter(descriptor -> descriptor.generation() == generation)
                .isPresent();
    }

    /** Confirms that the exact snapshot is reserved by a currently building generation. */
    default boolean isIndexSnapshotBuildable(String indexId, String snapshotId) {
        return false;
    }

    /** Atomically validates and activates one complete immutable generation. */
    IndexGenerationManifest publishIndexGeneration(IndexPublicationRequest request);

    /** Finds the active immutable generation manifest for an index. */
    Optional<IndexGenerationManifest> findActiveIndexGeneration(String indexId);

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

    /** Permanently deletes descriptor and all file mappings (for GC after grace period). */
    void purgeIndex(String indexId);

    // --- IndexFileMapping CRUD ---

    /** Registers a single index file mapping. */
    void registerIndexFile(IndexFileMapping mapping);

    /**
     * Batch-registers index file mappings. All mappings must belong to the same indexId.
     *
     * @throws IllegalArgumentException if mappings belong to different indexes
     */
    void batchRegisterIndexFiles(List<IndexFileMapping> mappings);

    /**
     * Copies reusable immutable objects into a staging generation and excludes
     * data files removed by the source-table commit. Returns copied object count.
     */
    default long copyIndexGenerationMappings(
            String indexId,
            int sourceGeneration,
            int targetGeneration,
            List<String> excludedDataFiles) {
        if (targetGeneration != Math.addExact(sourceGeneration, 1)) {
            throw new IllegalArgumentException(
                    "targetGeneration must advance exactly one generation");
        }
        java.util.Set<String> excluded = excludedDataFiles == null
                ? java.util.Set.of() : java.util.Set.copyOf(excludedDataFiles);
        List<IndexFileMapping> copies = getIndexFileMappings(indexId).stream()
                .filter(mapping -> mapping.generation() == sourceGeneration)
                .filter(mapping -> !excluded.contains(mapping.dataFilePath()))
                .map(mapping -> new IndexFileMapping(
                        mapping.indexId(), mapping.dataFilePath(), mapping.indexFilePath(),
                        mapping.rowCount(), mapping.indexSizeBytes(), mapping.checksum(),
                        targetGeneration, mapping.snapshotToken(), mapping.sequenceNum(),
                        mapping.createdAt(), mapping.lastVerifiedAt()))
                .toList();
        batchRegisterIndexFiles(copies);
        return copies.size();
    }

    /** Returns all file mappings for an index. */
    List<IndexFileMapping> getIndexFileMappings(String indexId);

    /**
     * Batch-fetches index file mappings for multiple indexes and data file paths.
     * Returns indexId → (dataFilePath → indexFilePath).
     */
    Map<String, Map<String, String>> batchGetIndexFileMappings(
            List<String> indexIds, List<String> dataFilePaths);

    /**
     * Returns the count of matching file mappings per index for the given data files.
     * Used as a lightweight pre-check before fetching full mappings, reducing memory
     * from O(indexes × files × pathLength) to O(indexes) for the initial coverage check.
     *
     * @param indexIds       index identifiers to count
     * @param dataFilePaths  data file paths to match against
     * @return indexId → count of matching data files; missing entries mean zero
     */
    default Map<String, Integer> getIndexFileMappingCounts(
            List<String> indexIds, List<String> dataFilePaths) {
        // Default: derive from full fetch (impls should override for efficiency)
        Map<String, Map<String, String>> full = batchGetIndexFileMappings(indexIds, dataFilePaths);
        Map<String, Integer> counts = new java.util.HashMap<>();
        for (var entry : full.entrySet()) {
            counts.put(entry.getKey(), entry.getValue().size());
        }
        return counts;
    }

    /**
     * Batch-fetches index file mappings filtered by snapshot token.
     * Only returns mappings whose opaque snapshot token exactly matches the
     * query snapshot token.
     * Returns indexId → (dataFilePath → indexFilePath).
     */
    default Map<String, Map<String, String>> batchGetIndexFileMappings(
            List<String> indexIds, List<String> dataFilePaths, String querySnapshotToken) {
        Map<String, Map<String, String>> result = new java.util.HashMap<>();
        java.util.Set<String> requestedPaths = java.util.Set.copyOf(dataFilePaths);
        for (String indexId : indexIds) {
            Map<String, String> selected = new java.util.HashMap<>();
            for (IndexFileMapping mapping : getIndexFileMappings(indexId)) {
                if (requestedPaths.contains(mapping.dataFilePath())
                        && java.util.Objects.equals(
                                querySnapshotToken, mapping.snapshotToken())) {
                    String previous = selected.put(
                            mapping.dataFilePath(), mapping.indexFilePath());
                    if (previous != null) {
                        throw new IllegalStateException(
                                "Duplicate index mapping for snapshot: indexId="
                                        + indexId + " dataFile="
                                        + mapping.dataFilePath());
                    }
                }
            }
            if (!selected.isEmpty()) {
                result.put(indexId, Map.copyOf(selected));
            }
        }
        return Map.copyOf(result);
    }

    /**
     * Fetches full immutable file metadata for exact query-pinned index generations.
     * Snapshot tokens are opaque identities and therefore use equality, never
     * lexical or numeric ordering.
     */
    default Map<String, Map<String, IndexFileMapping>> batchGetIndexArtifacts(
            Map<String, IndexFileSelection> selections,
            List<String> dataFilePaths) {
        if (selections.isEmpty() || dataFilePaths.isEmpty()) {
            return Map.of();
        }
        java.util.Set<String> requestedPaths = java.util.Set.copyOf(dataFilePaths);
        Map<String, Map<String, IndexFileMapping>> result = new java.util.HashMap<>();
        for (var selection : selections.entrySet()) {
            Map<String, IndexFileMapping> selected = IndexGenerationSelectionSupport.selectArtifacts(
                    this, selection.getKey(), selection.getValue(), requestedPaths);
            if (!selected.isEmpty()) {
                result.put(selection.getKey(), Map.copyOf(selected));
            }
        }
        return Map.copyOf(result);
    }

    /** Removes a single file mapping. */
    void removeIndexFileMapping(String indexId, String dataFilePath);

    /**
     * Removes a file mapping only if the index is in the expected state.
     * Prevents TOCTOU race between GC and concurrent state transitions.
     *
     * @param mapping       the exact file and generation to remove
     * @param expectedState the state the index must be in for removal to proceed
     * @param expectedGeneration the observed active index generation
     * @return true if the mapping was removed, false if the index was not in the expected state
     */
    default boolean removeIndexFileMappingIfState(IndexFileMapping mapping,
                                                   IndexState expectedState,
                                                   int expectedGeneration) {
        throw new UnsupportedOperationException("Exact generation-fenced mapping removal is required");
    }

    /** Returns data file paths not yet indexed for the given index. */
    List<String> filterUnindexedFiles(String indexId, List<String> dataFilePaths);

    /** Marks mappings as orphaned for GC cleanup. */
    void markOrphanedByDataFiles(String indexId, List<String> dataFilePaths);

    /** Returns mappings where lastVerifiedAt is older than the threshold. */
    List<IndexFileMapping> listStaleFileMappings(long olderThanMillis);

    /**
     * Returns mappings where lastVerifiedAt is older than the threshold
     * and createdAt is before the given cutoff. This prevents GC from
     * deleting recently-created mappings that may still be in-flight.
     */
    default List<IndexFileMapping> listStaleFileMappings(long olderThanMillis, Instant createdBefore) {
        return listStaleFileMappings(olderThanMillis);
    }

    /** Updates lastVerifiedAt for GC scan confirmation. */
    void updateLastVerifiedAt(String indexId, String dataFilePath, long timestamp);

    // --- Schema evolution support ---

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

    // --- State query support ---

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

    // --- Snapshot support ---

    /** Updates the last indexed snapshot ID for an index. */
    void updateLastIndexedSnapshotId(String indexId, String snapshotId);

    // --- Build claims (distributed coordination) ---

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

    // --- Leader election (removed: migrated to DistributedLease SPI) ---

    // --- Commit log ---

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

    // --- Cache events ---

    /** Inserts a cache invalidation event. */
    default void insertCacheEvent(String eventType, String indexId,
                                  String catalog, String schema, String table, Instant now) {}

    /** Polls cache events after the given event ID. */
    default List<CacheEvent> pollCacheEvents(long afterEventId, int limit) { return List.of(); }

    // --- Circuit breaker state ---

    /** Gets the circuit breaker state for an index. */
    default Optional<BreakerState> getBreakerState(String indexId) { return Optional.empty(); }

    /** Updates the circuit breaker state. Uses upsert semantics. */
    default void updateBreakerState(String indexId, int failureCount,
                                    Instant lastFailureAt, Instant lastSuccessAt, String state) {}

    // --- Paginated query ---

    /** Finds indexes by state with pagination. */
    default List<IndexDescriptor> findIndexesByStatePaginated(IndexState state, int offset, int limit) {
        return findIndexesByState(state);
    }

    // --- Generation fencing ---

    /** Deletes file mappings for generations <= maxGeneration. */
    default void deleteFileMappingsByGeneration(String indexId, int maxGeneration) {}

    // --- File column statistics (L0 pruning) ---

    /** Batch-inserts file column statistics for L0 partition pruning. */
    /**
     * Returns the number of indexed data files for each given index.
     * Used by the planner to compute coverage ratios for cost-based hint filtering.
     *
     * @param indexIds index identifiers to count
     * @return indexId → indexed file count; missing entries mean zero
     */
    default Map<String, Integer> getIndexedFileCounts(List<String> indexIds) {
        // Default: derive from getIndexFileMappings (N queries — impls should override)
        Map<String, Integer> result = new java.util.HashMap<>();
        for (String id : indexIds) {
            result.put(id, getIndexFileMappings(id).size());
        }
        return Map.copyOf(result);
    }

    /** Atomically replaces each complete file/snapshot statistics set within the exact tenant and table. */
    default void batchInsertFileStatistics(
            String tenantId, String catalog, String schema, String table,
            List<org.pangrova.connector.spi.index.statistics.DataFileStatistics> statistics) {}

    /**
     * Deletes file column statistics for the specified data file paths.
     * Called when files are removed (OVERWRITE/COMPACT) to prevent stale statistics accumulation.
     *
     * @param catalog   catalog name
     * @param tenantId authenticated owning tenant; never inferred from a catalog or file path
     * @param schema    schema name
     * @param table     table name
     * @param dataFilePaths paths of removed data files
     */
    default void deleteFileStatistics(String tenantId, String catalog, String schema, String table,
                                      List<String> dataFilePaths) {}

    /**
     * Returns column-level statistics for the specified data files and columns.
     * Returns dataFilePath → list of column statistics for the exact tenant and source snapshot only.
     * Unscoped historical rows are not query authority and must not be returned.
     */
    default Map<String, List<org.pangrova.connector.spi.index.statistics.FileColumnStatistics>> getFileStatistics(
            String tenantId, String catalog, String schema, String table, String snapshotId,
            List<String> dataFilePaths, List<String> columns) {
        return Map.of();
    }
}
