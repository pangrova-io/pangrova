package org.pangrova.coordinator.indexing.lifecycle;

import org.pangrova.indexing.api.descriptor.IndexDescriptor;
import org.pangrova.persistence.indexing.api.metadata.IndexGenerationManifest;
import org.pangrova.persistence.indexing.api.metadata.IndexMetadataStore;

import java.util.Objects;
import java.util.Set;

/** Selects exact scalar mappings or validated durable Native source coverage. */
final class IndexGenerationCoverageReader {

    private final IndexMetadataStore store;

    IndexGenerationCoverageReader(IndexMetadataStore store) {
        this.store = Objects.requireNonNull(store, "store");
    }

    Set<String> read(IndexDescriptor descriptor, IndexGenerationManifest active) {
        return switch (descriptor.indexType().physicalProfile()) {
            case NATIVE_RETRIEVAL -> readNative(descriptor, active);
            case SCALAR_FILE_PRUNING -> readScalar(descriptor, active);
        };
    }

    private Set<String> readNative(IndexDescriptor descriptor, IndexGenerationManifest active) {
        if (active == null) {
            throw new IllegalStateException(
                    "Native index has no active generation publication: " + descriptor.indexId());
        }
        return Set.copyOf(store.getNativeIndexSourcePaths(descriptor, active));
    }

    private Set<String> readScalar(IndexDescriptor descriptor, IndexGenerationManifest active) {
        int generation = active == null ? descriptor.generation() : active.generation();
        return Set.copyOf(store.getIndexedDataFilePaths(descriptor.indexId(), generation));
    }
}
