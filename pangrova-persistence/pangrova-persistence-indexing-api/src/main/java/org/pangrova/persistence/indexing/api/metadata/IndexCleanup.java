package org.pangrova.persistence.indexing.api.metadata;

import org.pangrova.common.api.annotation.Experimental;
import org.pangrova.indexing.api.enums.IndexState;
import java.time.Instant;
import java.util.List;

/** Physical cleanup and verification of obsolete index generations. */
@Experimental
public interface IndexCleanup {
    /** Permanently deletes descriptor and all file mappings (for GC after grace period). */
    void purgeIndex(String indexId);

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
        return listStaleFileMappings(olderThanMillis).stream()
                .filter(mapping -> mapping.createdAt().isBefore(createdBefore))
                .toList();
    }

    /** Updates lastVerifiedAt for GC scan confirmation. */
    void updateLastVerifiedAt(String indexId, String dataFilePath, long timestamp);

    /** Deletes file mappings for generations <= maxGeneration. */
    default void deleteFileMappingsByGeneration(String indexId, int maxGeneration) {
        throw new UnsupportedOperationException("Generation cleanup unavailable");
    }
}
