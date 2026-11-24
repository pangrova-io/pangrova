package org.pangrova.persistence.indexing.api.metadata;

import org.pangrova.common.api.annotation.Experimental;
import java.util.List;
import java.util.Map;
import java.util.Set;

/** Fenced writes and reads of data-file to index-artifact mappings. */
@Experimental
public interface IndexFileMappings {
    /**
     * Registers one mapping in a currently buildable, unpublished generation.
     * The same claim and publication fences apply to a single mapping as to a batch.
     * @throws IllegalStateException if the generation is no longer writable
     */
    default void registerIndexFile(String attemptOwner, IndexFileMapping mapping) {
        batchRegisterIndexFiles(attemptOwner, List.of(mapping));
    }

    /**
     * Atomically registers mappings for one index and generation, serialized with publication.
     * The writer must lock and verify the current build claim against attemptOwner within the
     * mapping transaction and retain the lock through commit. Expiration and release serialize
     * with the insert. A failed batch leaves no prefix. Published generations reject writes.
     *
     * @throws IllegalArgumentException if mappings belong to different indexes or generations
     * @throws IllegalStateException if the generation is no longer writable
     */
    void batchRegisterIndexFiles(String attemptOwner, List<IndexFileMapping> mappings);

    /**
     * Copies reusable immutable objects into a staging generation and excludes
     * data files removed by the source-table commit. Returns copied object count.
     * Source must be the active published generation, and target must have a BUILDING
     * reservation. State validation, copy and exclusions share one transaction,
     * serialized with publication. Implementations must not emulate this with separate reads/writes.
     */
    default long copyIndexGenerationMappings(
            String indexId,
            int sourceGeneration,
            int targetGeneration,
            List<String> excludedDataFiles) {
        throw new UnsupportedOperationException("Atomic generation-fenced mapping copy is required");
    }

    /** Returns all file mappings for an index. */
    List<IndexFileMapping> getIndexFileMappings(String indexId);

    /** Reads only source file paths from one exact index generation for maintenance. */
    Set<String> getIndexedDataFilePaths(String indexId, int generation);

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

    /** Returns data file paths not yet indexed for the given index. */
    List<String> filterUnindexedFiles(String indexId, List<String> dataFilePaths);

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
}
