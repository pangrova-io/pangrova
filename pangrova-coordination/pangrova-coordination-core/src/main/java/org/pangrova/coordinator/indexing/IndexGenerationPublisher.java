package org.pangrova.coordinator.indexing;

import java.time.Instant;

import org.pangrova.indexing.codec.data.IndexSerializer;
import org.pangrova.persistence.indexing.api.metadata.IndexMetadataStore;
import org.pangrova.persistence.indexing.api.metadata.IndexGenerationFormat;
import org.pangrova.persistence.indexing.api.metadata.IndexGenerationManifest;
import org.pangrova.persistence.indexing.api.metadata.IndexPublicationRequest;
import org.pangrova.indexing.api.descriptor.IndexDescriptor;

/** Creates the canonical artifact identity and atomically publishes one index generation. */
public final class IndexGenerationPublisher {

    private IndexGenerationPublisher() {}

    /** Publishes a complete generation after all expected objects are durable. */
    public static IndexGenerationManifest publish(
            IndexMetadataStore store,
            IndexDescriptor descriptor,
            long expectedObjects) {
        IndexGenerationFormat format = new IndexGenerationFormat(
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
