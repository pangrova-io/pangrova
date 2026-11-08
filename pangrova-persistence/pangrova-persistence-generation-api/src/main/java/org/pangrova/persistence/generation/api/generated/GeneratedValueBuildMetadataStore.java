package org.pangrova.persistence.generation.api.generated;

import java.util.Optional;

/** Metadata sidecar for generated-column artifact identity and snapshot selection. */
public interface GeneratedValueBuildMetadataStore {

    /** Registers or refreshes a generated-column identity before durable build submission. */
    void register(GeneratedValueBuildDescriptor descriptor);

    /** Finds the published immutable generation for one exact table snapshot and target. */
    Optional<GeneratedValueGenerationReference> findGeneration(
            String tenantId, String sourceId, String targetColumn, String sourceSnapshot);
}
