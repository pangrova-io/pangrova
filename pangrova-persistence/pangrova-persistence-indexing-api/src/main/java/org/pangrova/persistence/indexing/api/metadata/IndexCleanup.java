package org.pangrova.persistence.indexing.api.metadata;

import org.pangrova.common.api.annotation.Experimental;
import org.pangrova.indexing.api.enums.IndexState;
import java.time.Instant;
import java.util.List;
import java.util.Set;

/** Physical cleanup and verification of obsolete index generations. */
@Experimental
public interface IndexCleanup {
    /** Permanently deletes descriptor and all file mappings (for GC after grace period). */
    void purgeIndex(String indexId);

    /** Removes a single file mapping. */
    void removeIndexFileMapping(String indexId, String dataFilePath);

    /** Removes exactly the observed obsolete mappings under the expected descriptor fence.
     * The returned paths have no surviving mapping in any generation of the index.
     */
    default OrphanRemoval removeOrphanMappings(String indexId, IndexState expectedState,
                                              int expectedGeneration, List<IndexFileMapping> mappings) {
        throw new UnsupportedOperationException("Exact generation-fenced mapping removal is required");
    }

    record OrphanRemoval(int mappingsRemoved, Set<String> pathsToDelete) {
        public OrphanRemoval {
            pathsToDelete = Set.copyOf(pathsToDelete);
        }
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
