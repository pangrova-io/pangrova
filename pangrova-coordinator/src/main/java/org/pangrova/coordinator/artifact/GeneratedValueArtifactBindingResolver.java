package org.pangrova.coordinator.artifact;

import org.pangrova.metadata.artifact.contract.ArtifactTypeIds;
import org.pangrova.metadata.artifact.GeneratedValueArtifactMetadataStore;
import org.pangrova.metadata.artifact.GeneratedValueArtifactReference;
import org.pangrova.protocol.BuiltInCapabilityCatalog;
import org.pangrova.protocol.v2.OperatorBinding;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

/** Resolves an exact generated-value sidecar into the common operator binding contract. */
public final class GeneratedValueArtifactBindingResolver {

    private final GeneratedValueArtifactMetadataStore metadataStore;
    private final PublishedArtifactBindingResolver artifactResolver;
    private final org.pangrova.metadata.artifact.build.ArtifactBuildStore buildStore;

    /** Resolves generated templates through the same immutable artifact metadata and partition reader. */
    public GeneratedValueArtifactBindingResolver(
            GeneratedValueArtifactMetadataStore metadataStore,
            org.pangrova.metadata.artifact.build.ArtifactBuildStore buildStore) {
        this.metadataStore = Objects.requireNonNull(
                metadataStore, "metadataStore must not be null");
        this.artifactResolver = new PublishedArtifactBindingResolver(buildStore);
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
        GeneratedValueArtifactReference reference = metadataStore.findGeneration(
                tenantId, sourceId, targetColumn, sourceSnapshot).orElse(null);
        if (reference == null) {
            return Optional.empty();
        }
        var artifact = artifactResolver.resolveTemplate(
                        new PublishedArtifactBindingResolver.ArtifactResolutionIntent(
                                reference.tenantId(),
                                reference.artifactId(),
                                ArtifactTypeIds.GENERATED_COLUMN_VALUES,
                                reference.generation(), reference.sourceId(),
                                reference.sourceSnapshot(),
                                PublishedArtifactBindingResolver.SourceCoverage.QUERY_SOURCE_SUBSET), serializedSplits)
                .orElseThrow(() -> new IllegalStateException(
                        "Published generated values do not exactly cover the query source objects"));
        return Optional.of(template(tenantId, planNodeId, artifact));
    }

    private OperatorBinding template(String tenant, String node, org.pangrova.protocol.v2.ArtifactBinding artifact) {
        var manifest = buildStore.findPublishedGeneration(tenant, artifact.getArtifactId(), artifact.getGeneration(),
                artifact.getSourceSnapshot()).orElseThrow();
        if (manifest.rowCount() <= 0 || manifest.totalBytes() <= 0) {
            throw new IllegalStateException("Generated publication has invalid source-role resource totals");
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
