package org.pangrova.coordinator.generation.read;

import org.pangrova.generation.publication.enums.GenerationSourceCoverage;
import org.pangrova.generation.publication.read.PublishedGenerationBindingResolver;
import org.pangrova.generation.api.constants.GenerationOutputTypeIds;
import org.pangrova.persistence.generation.api.generated.GeneratedValueBuildMetadataStore;
import org.pangrova.persistence.generation.api.generated.GeneratedValueGenerationReference;
import org.pangrova.protocol.capability.BuiltInCapabilityCatalog;
import org.pangrova.protocol.v2.OperatorBinding;
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
     * Returns an object-free plan binding only for the exact source snapshot.
     * Task-local object expansion is delegated to PublishedArtifactBindingResolver.
     */
    public Optional<ResolvedRead> resolveTemplate(
            String tenantId,
            String planNodeId,
            String sourceId,
            String targetColumn,
            String sourceSnapshot,
            List<org.pangrova.protocol.v2.DataSplit> splits) {
        requireText(tenantId, "tenantId");
        requireText(planNodeId, "planNodeId");
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
        return Optional.of(new ResolvedRead(template(planNodeId, resolved), resolved.definitionSnapshot()));
    }

    /** Keeps the verified definition with the operator binding until the selected scan is checked. */
    public record ResolvedRead(OperatorBinding binding, Optional<DefinitionSnapshot> definitionSnapshot) {}

    private OperatorBinding template(
            String node, PublishedGenerationBindingResolver.ResolvedTemplate resolved) {
        if (resolved.rowCount() <= 0 || resolved.totalBytes() <= 0) {
            throw new IllegalStateException("Generated publication has invalid source-row resource totals");
        }
        return OperatorBinding.newBuilder().setPlanNodeId(node)
                .setCapability(BuiltInCapabilityCatalog.generatedValuesOverlayPlanRef())
                .addArtifacts(resolved.binding())
                .setExecutionPolicy(org.pangrova.protocol.v2.ExecutionPolicy.newBuilder()
                        .setCoveragePolicy(org.pangrova.protocol.v2.CoveragePolicy.COVERAGE_POLICY_REQUIRE_COMPLETE)
                        .setFallbackPolicy(org.pangrova.protocol.v2.FallbackPolicy.FALLBACK_POLICY_FORBID)
                        .setRetryClass(org.pangrova.protocol.v2.RetryClass.RETRY_CLASS_IDEMPOTENT))
                .setResources(org.pangrova.protocol.v2.OperatorResourceRequirement.newBuilder()
                        .setFillKeys(resolved.rowCount()).setObjectStoreReadBytes(resolved.totalBytes()))
                .build();
    }

    /** Uses the canonical task-local object expansion shared by all artifact capabilities. */
    public List<OperatorBinding> materializeForTask(
            String tenantId, List<OperatorBinding> bindings,
            List<org.pangrova.protocol.v2.DataSplit> splits) {
        return artifactResolver.materializeForTask(tenantId, bindings, splits);
    }
}
