package org.pangrova.coordinator.index;

import java.time.Instant;

import org.pangrova.indexing.serde.IndexSerializer;
import org.pangrova.metadata.index.IndexMetadataStore;
import org.pangrova.metadata.index.model.IndexArtifactFormat;
import org.pangrova.metadata.index.model.IndexGenerationManifest;
import org.pangrova.metadata.index.model.IndexPublicationRequest;
import org.pangrova.spi.index.IndexDescriptor;

/** Creates the canonical artifact identity and atomically publishes one index generation. */
public final class IndexGenerationPublisher {

    private IndexGenerationPublisher() {}

    /** Publishes a complete generation after all expected objects are durable. */
    public static IndexGenerationManifest publish(
            IndexMetadataStore store,
            IndexDescriptor descriptor,
            long expectedObjects) {
        IndexArtifactFormat format = new IndexArtifactFormat(
                IndexSerializer.ARTIFACT_FORMAT_ID,
                IndexSerializer.ARTIFACT_FORMAT_MAJOR_VERSION,
                IndexSerializer.ARTIFACT_FORMAT_MINOR_VERSION,
                IndexSerializer.ARTIFACT_FORMAT_SCHEMA_SHA256,
                IndexSerializer.WRITER_BUILD_ID);
        return store.publishIndexGeneration(new IndexPublicationRequest(
                descriptor.indexId(), descriptor.indexId(), descriptor.generation(),
                descriptor.buildSnapshotId(), descriptor.indexType().name(),
                format, expectedObjects, descriptor.updatedAt(), Instant.now()));
    }
}
