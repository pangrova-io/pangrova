package org.pangrova.indexing.store;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/** Validation helpers for exact generation-pinned artifact selection. */
final class IndexGenerationSelectionSupport {

    private IndexGenerationSelectionSupport() {}

    static Map<String, IndexFileMapping> selectArtifacts(
            IndexMetadataStore store,
            String indexId,
            IndexFileSelection selection,
            Set<String> requestedPaths) {
        var active = store.findActiveIndexGeneration(indexId);
        if (active.isEmpty() || !selection.matches(active.get())) {
            return Map.of();
        }
        Map<String, IndexFileMapping> selected = new HashMap<>();
        for (IndexFileMapping mapping : store.getIndexFileMappings(indexId)) {
            if (requestedPaths.contains(mapping.dataFilePath()) && selection.matches(mapping)) {
                putUniqueMapping(selected, indexId, mapping);
            }
        }
        return selected;
    }

    static void putUniqueMapping(
            Map<String, IndexFileMapping> selected,
            String indexId,
            IndexFileMapping mapping) {
        IndexFileMapping previous = selected.put(mapping.dataFilePath(), mapping);
        if (previous != null) {
            throw new IllegalStateException(
                    "Duplicate index artifact for generation: indexId="
                            + indexId + " dataFile=" + mapping.dataFilePath());
        }
    }
}
