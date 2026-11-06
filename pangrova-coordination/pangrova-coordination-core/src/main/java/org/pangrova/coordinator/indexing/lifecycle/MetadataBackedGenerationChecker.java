package org.pangrova.coordinator.indexing.lifecycle;

import org.pangrova.indexing.build.GenerationChecker;
import org.pangrova.persistence.indexing.api.metadata.IndexMetadataStore;

import static org.pangrova.common.util.Preconditions.checkNotNull;

/**
 * Validates index generation currency by querying the metadata store.
 * Prevents stale build results from overwriting newer data after a
 * concurrent rebuild has incremented the generation counter.
 */
public final class MetadataBackedGenerationChecker implements GenerationChecker {

    private final IndexMetadataStore metadataStore;

    public MetadataBackedGenerationChecker(IndexMetadataStore metadataStore) {
        this.metadataStore = checkNotNull(metadataStore, "metadataStore must not be null");
    }

    @Override
    public boolean isCurrentGeneration(String indexId, int expectedGeneration) {
        return metadataStore.isIndexGenerationBuildable(indexId, expectedGeneration);
    }
}
