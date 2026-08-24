package org.pangrova.metadata.index;

import org.pangrova.metadata.exception.IndexAlreadyExistsException;
import org.pangrova.metadata.index.model.BreakerState;
import org.pangrova.metadata.index.model.CacheEvent;
import org.pangrova.metadata.index.model.CommitLogEntry;
import org.pangrova.metadata.index.model.IndexFileMapping;
import org.pangrova.spi.index.IndexDescriptor;
import org.pangrova.spi.index.IndexState;

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

    /** Finds an index by its unique identifier. */
    Optional<IndexDescriptor> findIndex(String indexId);

    /** Lists all indexes for a given table. */
    List<IndexDescriptor> findIndexesByTable(String catalog, String schema, String table);

    /**
     * Returns READY (and optionally STALE) indexes covering at least one
     * of the specified columns.
     */
    List<IndexDescriptor> findReadyIndexesForColumns(
            String catalog, String schema, String table,
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
     * Only returns mappings where snapshotToken &lt;= querySnapshotToken.
     * Returns indexId → (dataFilePath → indexFilePath).
     */
    default Map<String, Map<String, String>> batchGetIndexFileMappings(
            List<String> indexIds, List<String> dataFilePaths, String querySnapshotToken) {
        // Default: ignore snapshot filtering for backward compatibility
        return batchGetIndexFileMappings(indexIds, dataFilePaths);
    }

    /** Removes a single file mapping. */
    void removeIndexFileMapping(String indexId, String dataFilePath);

    /**
     * Removes a file mapping only if the index is in the expected state.
     * Prevents TOCTOU race between GC and concurrent state transitions.
     *
     * @param indexId       the index identifier
     * @param dataFilePath  the data file path to remove
     * @param expectedState the state the index must be in for removal to proceed
     * @return true if the mapping was removed, false if the index was not in the expected state
     */
    default boolean removeIndexFileMappingIfState(String indexId, String dataFilePath,
                                                   IndexState expectedState) {
        removeIndexFileMapping(indexId, dataFilePath);
        return true;
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
            String catalog, String schema, String table, String column);

    /** Renames a column across all indexes for a table. */
    void renameColumnInIndexes(
            String catalog, String schema, String table,
            String oldColumn, String newColumn);

    /** Drops all indexes for a table. */
    void dropAllIndexesForTable(String catalog, String schema, String table, Instant timestamp);

    /** Renames a table across all its indexes. */
    void renameTable(String catalog, String schema, String oldTable, String newTable);

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
    default void recordCommitEvent(CommitEventRequest request) {}

    /** Polls unprocessed commit events, ordered by creation time. */
    default List<CommitLogEntry> pollUnprocessedCommitEvents(int limit) { return List.of(); }

    /** Marks a commit event as processed. */
    default void markCommitEventProcessed(long eventId) {}

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

    default void batchInsertFileStatistics(
            String catalog, String schema, String table,
            List<org.pangrova.spi.index.statistics.DataFileStatistics> statistics) {}

    /**
     * Deletes file column statistics for the specified data file paths.
     * Called when files are removed (OVERWRITE/COMPACT) to prevent stale statistics accumulation.
     *
     * @param catalog   catalog name
     * @param schema    schema name
     * @param table     table name
     * @param dataFilePaths paths of removed data files
     */
    default void deleteFileStatistics(String catalog, String schema, String table,
                                      List<String> dataFilePaths) {}

    /**
     * Returns column-level statistics for the specified data files and columns.
     * Returns dataFilePath → list of column statistics.
     */
    default Map<String, List<org.pangrova.spi.index.statistics.FileColumnStatistics>> getFileStatistics(
            String catalog, String schema, String table,
            List<String> dataFilePaths, List<String> columns) {
        return Map.of();
    }
}
