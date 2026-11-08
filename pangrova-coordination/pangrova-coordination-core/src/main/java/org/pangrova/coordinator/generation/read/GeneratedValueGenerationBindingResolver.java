package org.pangrova.coordinator.generation.read;

import org.pangrova.generation.core.publication.read.PublishedGenerationBindingResolver;

import org.pangrova.generation.api.output.GenerationOutputTypeIds;
import org.pangrova.persistence.generation.api.generated.GeneratedValueBuildMetadataStore;
import org.pangrova.persistence.generation.api.generated.GeneratedValueGenerationReference;
import org.pangrova.protocol.capability.BuiltInCapabilityCatalog;
import org.pangrova.protocol.v2.OperatorBinding;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

/** Resolves an exact generated-value sidecar into the common operator binding contract. */
public final class GeneratedValueGenerationBindingResolver {

    private final GeneratedValueBuildMetadataStore metadataStore;
    private final PublishedGenerationBindingResolver artifactResolver;
    private final org.pangrova.persistence.generation.api.job.GenerationJobStore buildStore;

    /** Resolves generated templates through the same immutable artifact metadata and partition reader. */
    public GeneratedValueGenerationBindingResolver(
            GeneratedValueBuildMetadataStore metadataStore,
            org.pangrova.persistence.generation.api.job.GenerationJobStore buildStore) {
        this.metadataStore = Objects.requireNonNull(
                metadataStore, "metadataStore must not be null");
        this.artifactResolver = new PublishedGenerationBindingResolver(buildStore);
        this.buildStore = Objects.requireNonNull(buildStore, "buildStore");
    }

    /**
     * Returns an object-free plan binding only for the exact source snapshot.
     * Task-local object expansion is delegated to PublishedArtifactBindingResolver.
     */
    public Optional<OperatorBinding> resolveTemplate(
            String tenantId,
            String planNodeId,
            String sourceId,
            String targetColumn,
            String sourceSnapshot,
            List<String> serializedSplits) {
        requireText(tenantId, "tenantId");
        requireText(planNodeId, "planNodeId");
        GeneratedValueGenerationReference reference = metadataStore.findGeneration(
                tenantId, sourceId, targetColumn, sourceSnapshot).orElse(null);
        if (reference == null) {
            return Optional.empty();
        }
        var artifact = artifactResolver.resolveTemplate(
                        new PublishedGenerationBindingResolver.GenerationResolutionIntent(
                                reference.tenantId(),
                                reference.artifactId(),
                                GenerationOutputTypeIds.GENERATED_COLUMN_VALUES,
                                reference.generation(), reference.sourceId(),
                                reference.sourceSnapshot(),
                                PublishedGenerationBindingResolver.SourceCoverage.QUERY_SOURCE_SUBSET),
                        serializedSplits)
                .orElseThrow(() -> new IllegalStateException(
                        "Published generated values do not exactly cover the query source objects"));
        return Optional.of(template(tenantId, planNodeId, artifact));
    }

    private OperatorBinding template(String tenant, String node, org.pangrova.protocol.v2.ArtifactBinding artifact) {
        var manifest = buildStore.findPublishedGeneration(tenant, artifact.getArtifactId(), artifact.getGeneration(),
                artifact.getSourceSnapshot()).orElseThrow();
        if (manifest.rowCount() <= 0 || manifest.totalBytes() <= 0) {
            throw new IllegalStateException("Generated publication has invalid source-row resource totals");
        }
        return OperatorBinding.newBuilder().setPlanNodeId(node)
                .setCapability(BuiltInCapabilityCatalog.generatedValuesOverlayPlanRef()).addArtifacts(artifact)
                .setExecutionPolicy(org.pangrova.protocol.v2.ExecutionPolicy.newBuilder()
                        .setCoveragePolicy(org.pangrova.protocol.v2.CoveragePolicy.COVERAGE_POLICY_REQUIRE_COMPLETE)
                        .setFallbackPolicy(org.pangrova.protocol.v2.FallbackPolicy.FALLBACK_POLICY_FORBID)
                        .setRetryClass(org.pangrova.protocol.v2.RetryClass.RETRY_CLASS_IDEMPOTENT))
                .setResources(org.pangrova.protocol.v2.OperatorResourceRequirement.newBuilder()
                        .setFillKeys(manifest.rowCount()).setObjectStoreReadBytes(manifest.totalBytes()))
                .build();
    }

    /** Uses the canonical task-local object expansion shared by all artifact capabilities. */
    public List<OperatorBinding> materializeForTask(
            String tenantId, List<OperatorBinding> bindings,
            List<String> serializedSplits) {
        return artifactResolver.materializeForTask(
                tenantId, bindings, serializedSplits);
    }

    private static void requireText(String value, String name) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(name + " must not be blank");
        }
    }
}
