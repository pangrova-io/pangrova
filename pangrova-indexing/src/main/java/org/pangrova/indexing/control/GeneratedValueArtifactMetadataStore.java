package org.pangrova.indexing.control;

import java.util.Optional;
import java.util.List;
import org.pangrova.indexing.api.GeneratedValueArtifactDescriptor;
import org.pangrova.indexing.api.GeneratedValueArtifactReference;
import org.pangrova.indexing.api.GeneratedValueBuildSubmission;
import org.pangrova.spi.coordination.WriteCommitEntry;

/** Metadata sidecar for generated-column artifact identity and snapshot selection. */
public interface GeneratedValueArtifactMetadataStore {

    /** Registers or refreshes a generated-column identity before durable build submission. */
    void register(GeneratedValueArtifactDescriptor descriptor);

    /** Atomically persists every declared generation and acknowledges its exact COMMITTED write redo anchor. */
    void submitCommittedBuilds(WriteCommitEntry write, List<GeneratedValueBuildSubmission> builds);

    /** Finds the published immutable generation for one exact table snapshot and target. */
    Optional<GeneratedValueArtifactReference> findGeneration(
            String tenantId, String sourceId, String targetColumn, String sourceSnapshot);
}
