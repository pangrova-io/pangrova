package org.pangrova.indexing.control;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.pangrova.indexing.api.IndexFileMapping;
import org.pangrova.indexing.api.IndexFileSelection;
import org.pangrova.spi.index.IndexDescriptor;
import org.pangrova.spi.index.IndexState;

/**
 * Persistent store for index metadata: descriptors and file mappings.
 *
 * <p>Provides CRUD operations for {@link IndexDescriptor} lifecycle management
 * and {@link IndexFileMapping} tracking for build, GC, and evaluation.</p>
 *
 * <p>The interface composes the descriptor, file-mapping, and generation
 * authorities defined by {@link IndexDescriptorAuthority},
 * {@link IndexFileMappingAuthority}, and {@link IndexGenerationAuthority};
 * callers that only need one authority may depend on the narrower type.</p>
 */
public interface IndexMetadataStore extends IndexDescriptorAuthority,
        IndexFileMappingAuthority, IndexGenerationAuthority {

    /** Returns whether a generation is currently fenced for build output. */
    default boolean isIndexGenerationBuildable(String indexId, int generation) {
        return findIndex(indexId)
                .filter(descriptor -> descriptor.state() == IndexState.BUILDING)
                .filter(descriptor -> descriptor.generation() == generation)
                .isPresent();
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
        Set<String> requestedPaths = Set.copyOf(dataFilePaths);
        Map<String, Map<String, IndexFileMapping>> result = new HashMap<>();
        for (var selection : selections.entrySet()) {
            Map<String, IndexFileMapping> selected = IndexArtifactSelectionSupport.selectArtifacts(
                    this, selection.getKey(), selection.getValue(), requestedPaths);
            if (!selected.isEmpty()) {
                result.put(selection.getKey(), Map.copyOf(selected));
            }
        }
        return Map.copyOf(result);
    }
}
