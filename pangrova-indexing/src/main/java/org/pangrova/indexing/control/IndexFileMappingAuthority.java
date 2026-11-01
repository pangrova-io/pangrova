package org.pangrova.indexing.control;

import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import org.pangrova.indexing.api.IndexFileMapping;
import org.pangrova.spi.index.IndexState;
import org.pangrova.spi.index.statistics.DataFileStatistics;
import org.pangrova.spi.index.statistics.FileColumnStatistics;

/**
 * File-mapping authority of the index metadata store: mapping CRUD, GC
 * tracking, and file column statistics for L0 pruning.
 */
public interface IndexFileMappingAuthority {

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
        Set<String> excluded = excludedDataFiles == null
                ? Set.of() : Set.copyOf(excludedDataFiles);
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
        Map<String, Integer> counts = new HashMap<>();
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
        Map<String, Map<String, String>> result = new HashMap<>();
        Set<String> requestedPaths = Set.copyOf(dataFilePaths);
        for (String indexId : indexIds) {
            Map<String, String> selected = new HashMap<>();
            for (IndexFileMapping mapping : getIndexFileMappings(indexId)) {
                if (requestedPaths.contains(mapping.dataFilePath())
                        && Objects.equals(
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

    /** Deletes file mappings for generations <= maxGeneration. */
    default void deleteFileMappingsByGeneration(String indexId, int maxGeneration) {}

    /**
     * Returns the number of indexed data files for each given index.
     * Used by the planner to compute coverage ratios for cost-based hint filtering.
     *
     * @param indexIds index identifiers to count
     * @return indexId → indexed file count; missing entries mean zero
     */
    default Map<String, Integer> getIndexedFileCounts(List<String> indexIds) {
        // Default: derive from getIndexFileMappings (N queries — impls should override)
        Map<String, Integer> result = new HashMap<>();
        for (String id : indexIds) {
            result.put(id, getIndexFileMappings(id).size());
        }
        return Map.copyOf(result);
    }

    /** Atomically replaces each complete file/snapshot statistics set within the exact tenant and table. */
    default void batchInsertFileStatistics(
            String tenantId, String catalog, String schema, String table,
            List<DataFileStatistics> statistics) {}

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
    default Map<String, List<FileColumnStatistics>> getFileStatistics(
            String tenantId, String catalog, String schema, String table, String snapshotId,
            List<String> dataFilePaths, List<String> columns) {
        return Map.of();
    }
}
