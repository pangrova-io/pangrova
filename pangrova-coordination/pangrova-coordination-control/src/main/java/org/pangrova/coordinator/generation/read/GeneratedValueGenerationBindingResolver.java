package org.pangrova.coordinator.generation.read;

import org.pangrova.generation.publication.enums.GenerationSourceCoverage;
import org.pangrova.generation.publication.read.PublishedGenerationBindingResolver;
import org.pangrova.generation.api.constants.GenerationOutputTypeIds;
import org.pangrova.persistence.generation.api.generated.GeneratedValueBuildMetadataStore;
import org.pangrova.persistence.generation.api.generated.GeneratedValueGenerationReference;
import org.pangrova.protocol.capability.BuiltInCapabilityCatalog;
import org.pangrova.protocol.v2.ArtifactBinding;
import org.pangrova.protocol.v2.CoveragePolicy;
import org.pangrova.protocol.v2.DataSplit;
import org.pangrova.protocol.v2.ExecutionPolicy;
import org.pangrova.protocol.v2.FallbackPolicy;
import org.pangrova.protocol.v2.OperatorBinding;
import org.pangrova.protocol.v2.OperatorResourceRequirement;
import org.pangrova.protocol.v2.RetryClass;
import org.pangrova.security.api.authorization.DeferredAuthorizationScope.DefinitionSnapshot;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static org.pangrova.common.util.Preconditions.requireText;

/** Resolves an exact generated-value sidecar into the common operator binding contract. */
public final class GeneratedValueGenerationBindingResolver {

    private final GeneratedValueBuildMetadataStore metadataStore;
    private final PublishedGenerationBindingResolver artifactResolver;

    /** Resolves generated templates through the same immutable artifact metadata and partition reader. */
    public GeneratedValueGenerationBindingResolver(
            GeneratedValueBuildMetadataStore metadataStore,
            org.pangrova.persistence.generation.api.job.GenerationJobStore buildStore) {
        this.metadataStore = Objects.requireNonNull(
                metadataStore, "metadataStore must not be null");
        this.artifactResolver = new PublishedGenerationBindingResolver(buildStore);
    }

    /**
     * Returns verified, object-free publication evidence for the exact source snapshot.
     * The caller binds that evidence to each eligible read node.
     */
    public Optional<ResolvedRead> resolveTemplate(
            String tenantId,
            String sourceId,
            String targetColumn,
            String sourceSnapshot,
            List<DataSplit> splits) {
        requireText(tenantId, "tenantId");
        GeneratedValueGenerationReference reference = metadataStore.findGeneration(
                tenantId, sourceId, targetColumn, sourceSnapshot).orElse(null);
        if (reference == null) {
            return Optional.empty();
        }
        if (!reference.tenantId().equals(tenantId) || !reference.sourceId().equals(sourceId)
                || !reference.sourceSnapshot().equals(sourceSnapshot)
                || !reference.targetColumn().equalsIgnoreCase(targetColumn)) {
            throw new IllegalStateException("Generated publication reference differs from the query scope");
        }
        var resolved = artifactResolver.resolveTemplate(
                        new PublishedGenerationBindingResolver.GenerationResolutionIntent(
                                reference.tenantId(),
                                reference.artifactId(),
                                GenerationOutputTypeIds.GENERATED_COLUMN_VALUES,
                                reference.generation(), reference.sourceId(),
                                reference.sourceSnapshot(),
                                GenerationSourceCoverage.QUERY_SOURCE_SUBSET),
                        splits)
                .orElseThrow(() -> new IllegalStateException(
                        "Published generated values do not exactly cover the query source objects"));
        if (resolved.rowCount() <= 0 || resolved.totalBytes() <= 0) {
            throw new IllegalStateException("Generated publication has invalid source-row resource totals");
        }
        return Optional.of(new ResolvedRead(resolved.binding(), OperatorResourceRequirement.newBuilder()
                .setFillKeys(resolved.rowCount()).setObjectStoreReadBytes(resolved.totalBytes()).build(),
                resolved.definitionSnapshot()));
    }

    /** Immutable publication evidence reusable by scans of the same declaration and source snapshot. */
    public record ResolvedRead(ArtifactBinding artifact, OperatorResourceRequirement resources,
            Optional<DefinitionSnapshot> definitionSnapshot) {
        /** Attaches the shared publication to the exact scan's node ID. */
        public OperatorBinding bind(String planNodeId) {
            return OperatorBinding.newBuilder().setPlanNodeId(requireText(planNodeId, "planNodeId"))
                    .setCapability(BuiltInCapabilityCatalog.generatedValuesOverlayPlanRef())
                    .addArtifacts(artifact)
                    .setExecutionPolicy(ExecutionPolicy.newBuilder()
                            .setCoveragePolicy(CoveragePolicy.COVERAGE_POLICY_REQUIRE_COMPLETE)
                            .setFallbackPolicy(FallbackPolicy.FALLBACK_POLICY_FORBID)
                            .setRetryClass(RetryClass.RETRY_CLASS_IDEMPOTENT))
                    .setResources(resources)
                    .build();
        }
    }
}
