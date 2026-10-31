package org.pangrova.coordinator.databuild;

import com.google.protobuf.InvalidProtocolBufferException;
import org.pangrova.coordinator.query.execution.dispatch.dag.retrieval.RetrievalGenerationIntent;
import org.pangrova.databuild.api.format.DataBuildOutputFormatContracts.FormatContract;
import org.pangrova.databuild.api.manifest.DataBuildGenerationManifest;
import org.pangrova.databuild.api.build.DataBuildOutput;
import org.pangrova.databuild.api.build.DataBuildSplit;
import org.pangrova.databuild.api.DataBuildOutputScope;
import org.pangrova.databuild.api.build.DataBuildStore;
import org.pangrova.databuild.api.execution.template.DataBuildStageTemplate.PublicationRead;
import org.pangrova.databuild.store.jdbc.build.JdbcDataBuildStageExecutionStore.PublishedInput;
import org.pangrova.protocol.databuild.PublishedGenerationBindingChecksum;
import org.pangrova.protocol.v2.ArtifactBinding;
import org.pangrova.protocol.v2.ArtifactWriteBinding;
import org.pangrova.protocol.v2.DataSplit;
import org.pangrova.protocol.v2.OperatorBinding;
import org.pangrova.protocol.v2.OperatorBindingSet;

import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

/**
 * Resolves immutable artifact metadata on the Coordinator control plane.
 *
 * <p>The query-level resolution deliberately excludes physical objects. Those
 * are loaded only for one task partition, after split partitioning, so a large
 * generation is never broadcast through every task request.</p>
 */
public final class PublishedGenerationBindingResolver {

    private final DataBuildStore store;

    public PublishedGenerationBindingResolver(DataBuildStore store) {
        this.store = Objects.requireNonNull(store, "store must not be null");
    }

    /**
     * Pins a published generation if every stage split reads its exact source
     * snapshot. Absence or a snapshot mismatch means the caller must keep the
     * exact scan plan; malformed published metadata is an integrity failure.
     */
    public Optional<ResolvedRetrievalGeneration> resolveTemplate(
            RetrievalGenerationIntent intent, List<String> serializedSplits) {
        Objects.requireNonNull(intent, "intent must not be null");
        if (org.pangrova.protocol.capability.BuiltInCapabilityCatalog.FILTERED_DISKANN_RETRIEVAL_ID
                .equals(intent.capability().getCapabilityId())
                && !FilteredAnnSourceScope.supports(PublishedGenerationBindingContract.parseSplits(serializedSplits),
                        intent.sourceId(), intent.sourceSnapshot())) {
            // This runs before any TaskRequest or search starts: retain the actual
            // original SQL filter and the legal Native exact scan algorithm.
            return Optional.empty();
        }
        return resolveTemplate(new GenerationResolutionIntent(
                intent.tenantId(), intent.artifactId(),
                intent.artifactType(), intent.generation(),
                intent.sourceId(), intent.sourceSnapshot(), SourceCoverage.COMPLETE_GENERATION), serializedSplits)
                // resolveTemplate already required one unique source object per split.
                .map(binding -> new ResolvedRetrievalGeneration(intent, binding,
                        serializedSplits.size()));
    }

    /**
     * Resolves an object-free binding for any published immutable artifact.
     * Feature planners attach the returned generic binding to their own exact
     * capability; this layer has no vector/search/generated-value branching.
     */
    public Optional<ArtifactBinding> resolveTemplate(
            GenerationResolutionIntent intent, List<String> serializedSplits) {
        Objects.requireNonNull(intent, "intent must not be null");
        List<DataSplit> splits = PublishedGenerationBindingContract.parseSplits(
                serializedSplits);
        if (splits.isEmpty() || splits.stream().anyMatch(split ->
                !split.getSourceId().equals(intent.sourceId())
                        || !split.getSourceSnapshot().equals(intent.sourceSnapshot()))) {
            return Optional.empty();
        }
        Optional<Map<String, String>> splitIdentities = splitIdentities(splits);
        if (splitIdentities.isEmpty()) {
            return Optional.empty();
        }
        DataBuildGenerationManifest manifest = store.findPublishedGeneration(
                intent.tenantId(), intent.artifactId(),
                intent.generation(), intent.sourceSnapshot())
                .orElse(null);
        if (manifest == null) {
            return Optional.empty();
        }
        PublishedGenerationBindingContract.validateManifest(intent, manifest);
        FormatContract contract = PublishedGenerationBindingContract.requireFormatContract(
                manifest.formatId(), manifest.artifactType());
        long publishedSourceRoles = contract.sourceMapRoles().values().stream()
                .filter(role -> role.scope() == DataBuildOutputScope.SOURCE_OBJECT)
                .count();
        long expectedObjects = Math.addExact(
                Math.multiplyExact((long) splitIdentities.orElseThrow().size(),
                        publishedSourceRoles),
                contract.generationFinalizeRoles().size());
        if (!matchesCoverage(intent.coverage(), manifest.objectCount(), expectedObjects)
                || !matchesPublishedSourceRanges(manifest, splitIdentities.orElseThrow())) {
            return Optional.empty();
        }
        ArtifactWriteBinding writeTemplate = loadWriteTemplate(manifest);
        PublishedGenerationBindingContract.validateWriteTemplate(manifest, writeTemplate);
        return Optional.of(bindingTemplate(manifest, writeTemplate));
    }

    private static boolean matchesCoverage(SourceCoverage coverage, long published, long requested) {
        return coverage == SourceCoverage.COMPLETE_GENERATION ? published == requested : published >= requested;
    }

    private boolean matchesPublishedSourceRanges(
            DataBuildGenerationManifest manifest, Map<String, String> requested) {
        Map<String, DataBuildSplit> stored = store.loadPublishedSourceSplits(
                manifest.tenantId(), manifest.jobId(), requested.keySet());
        if (!stored.keySet().equals(requested.keySet())) return false;
        for (var entry : stored.entrySet()) {
            final DataSplit split;
            try {
                split = DataSplit.parseFrom(entry.getValue().canonicalSplitBytes());
            } catch (InvalidProtocolBufferException malformed) {
                throw new IllegalStateException("Published canonical source split is malformed", malformed);
            }
            if (!entry.getKey().equals(split.getDataObjectId())
                    || !org.pangrova.protocol.source.DataSplitProtoCodec.requireStableSplitId(split)
                            .equals(requested.get(entry.getKey()))) {
                return false;
            }
        }
        return true;
    }

    private static Optional<Map<String, String>> splitIdentities(
            List<DataSplit> splits) {
        Map<String, String> identities = new LinkedHashMap<>();
        for (DataSplit split : splits) {
            String splitId = org.pangrova.protocol.source.DataSplitProtoCodec.requireStableSplitId(split);
            if (identities.put(split.getDataObjectId(), splitId) != null) {
                return Optional.empty();
            }
        }
        return Optional.of(Map.copyOf(identities));
    }

    private static ArtifactBinding bindingTemplate(
            DataBuildGenerationManifest manifest, ArtifactWriteBinding writeTemplate) {
        return ArtifactBinding.newBuilder()
                .setArtifactId(manifest.artifactId())
                .setArtifactType(manifest.artifactType())
                .setGeneration(manifest.generation())
                .setSourceId(manifest.sourceId())
                .setSourceSnapshot(manifest.sourceSnapshot())
                .setArtifactCreatedAtEpochMs(manifest.publishedAt().toEpochMilli())
                .setReadEpoch(manifest.manifestSha256())
                .setManifestUri(manifest.manifestUri())
                .setManifestChecksum(PublishedGenerationBindingContract.sha256(
                        manifest.manifestSha256()))
                .setFormat(writeTemplate.getFormat())
                .addAllProperties(writeTemplate.getPropertiesList())
                .setRowLocator(writeTemplate.getRowLocator())
                .addAllDependencies(writeTemplate.getDependenciesList())
                .setDependencySetChecksum(writeTemplate.getDependencySetChecksum())
                .setExecutionKind(writeTemplate.getExecutionKind())
                .setEmptyPartitionPolicy(writeTemplate.getEmptyPartitionPolicy())
                .build();
    }

    /**
     * Expands artifact templates with only the objects required by one task's
     * split identities and signs the resulting immutable binding.
     */
    public List<OperatorBinding> materializeForTask(
            String tenantId, List<OperatorBinding> bindings,
            List<String> serializedSplits) {
        if (tenantId == null || tenantId.isBlank()) {
            throw new IllegalArgumentException("tenantId must not be blank");
        }
        if (bindings == null || bindings.isEmpty()) {
            return List.of();
        }
        List<DataSplit> splits = PublishedGenerationBindingContract.parseSplits(
                serializedSplits);
        List<OperatorBinding> result = new java.util.ArrayList<>(bindings.size());
        for (OperatorBinding binding : bindings) {
            if (binding.getArtifactsCount() == 0) {
                result.add(binding);
                continue;
            }
            OperatorBinding.Builder materialized = binding.toBuilder().clearArtifacts();
            for (ArtifactBinding artifact : binding.getArtifactsList()) {
                materialized.addArtifacts(materialize(tenantId, artifact, splits));
            }
            result.add(materialized.build());
        }
        return List.copyOf(result);
    }

    private ArtifactBinding materialize(
            String tenantId, ArtifactBinding template, List<DataSplit> splits) {
        if (template.getSplitObjectsCount() != 0 || template.hasBindingChecksum()) {
            throw new IllegalArgumentException(
                    "artifact plan binding must be an unsigned object-free template");
        }
        if (splits.isEmpty()) {
            throw new IllegalArgumentException(
                    "artifact retrieval cannot be materialized without task splits");
        }
        Set<String> sourceObjectIds = matchingSourceObjects(template, splits);
        DataBuildGenerationManifest manifest = store.findPublishedGeneration(
                tenantId, template.getArtifactId(), template.getGeneration(),
                template.getSourceSnapshot()).orElseThrow(() ->
                        new IllegalStateException(
                                "pinned artifact generation is no longer published"));
        List<DataBuildOutput> outputs = store.loadPublishedOutputs(
                manifest.tenantId(), manifest.jobId(), sourceObjectIds);
        return materializePrepared(template, splits, new PublishedInput(manifest, outputs, sourceObjectIds));
    }

    /** Pure task-local materialization from immutable evidence read on the claim transaction's connection. */
    public static List<OperatorBinding> materializePrepared(
            List<OperatorBinding> bindings, List<String> serializedSplits,
            Map<PublicationRead, PublishedInput> publications) {
        var splits = PublishedGenerationBindingContract.parseSplits(serializedSplits);
        Set<PublicationRead> required = bindings.stream().flatMap(binding -> binding.getArtifactsList().stream())
                .map(PublishedGenerationBindingResolver::publicationRead).collect(java.util.stream.Collectors.toSet());
        if (!required.equals(publications.keySet())) {
            throw new IllegalStateException(
                    "Prepared publication evidence differs from immutable stage read templates");
        }
        return bindings.stream().map(binding -> {
            var result = binding.toBuilder().clearArtifacts();
            for (var artifact : binding.getArtifactsList()) {
                result.addArtifacts(materializePrepared(artifact, splits, publications.get(publicationRead(artifact))));
            }
            return result.build();
        }).toList();
    }

    /** Extracts only an immutable published pointer; runtime objects, capabilities and credentials are forbidden. */
    public static PublicationRead publicationRead(ArtifactBinding artifact) {
        if (artifact.getSplitObjectsCount() != 0 || artifact.hasBindingChecksum()) {
            throw new IllegalArgumentException("Stage read template must be unsigned and object-free");
        }
        return new PublicationRead(artifact.getArtifactId(), artifact.getGeneration(), artifact.getSourceId(),
                artifact.getSourceSnapshot(), artifact.getReadEpoch());
    }

    private static Set<String> matchingSourceObjects(ArtifactBinding template, List<DataSplit> splits) {
        Set<String> result = new HashSet<>();
        for (DataSplit split : splits) {
            if (split.getSourceId().equals(template.getSourceId())) {
                if (!split.getSourceSnapshot().equals(template.getSourceSnapshot())) {
                    throw new IllegalArgumentException("task split differs from the pinned artifact source snapshot");
                }
                result.add(split.getDataObjectId());
            }
        }
        if (result.isEmpty()) throw new IllegalArgumentException("artifact source has no exact task-local splits");
        return Set.copyOf(result);
    }

    private static ArtifactBinding materializePrepared(ArtifactBinding template, List<DataSplit> splits,
            PublishedInput evidence) {
        publicationRead(template);
        Set<String> sourceObjectIds = matchingSourceObjects(template, splits);
        if (!sourceObjectIds.equals(evidence.sourceObjectIds())) {
            throw new IllegalStateException("Prepared publication source coverage differs from exact task inputs");
        }
        PublishedGenerationBindingContract.validateTemplateAgainstManifest(template, evidence.manifest());
        List<DataBuildOutput> outputs = evidence.outputs();
        PublishedGenerationBindingContract.validateObjectCoverage(
                template, sourceObjectIds, outputs);

        ArtifactBinding.Builder builder = template.toBuilder();
        outputs.stream()
                .sorted(java.util.Comparator
                        .comparing(DataBuildOutput::sourceObjectId)
                        .thenComparingInt(DataBuildOutput::ordinal)
                        .thenComparing(DataBuildOutput::objectId))
                .map(PublishedGenerationBindingContract::toProtocolObject)
                .forEach(builder::addSplitObjects);
        return PublishedGenerationBindingChecksum.sign(builder.build());
    }

    private ArtifactWriteBinding loadWriteTemplate(
            DataBuildGenerationManifest manifest) {
        byte[] bytes = store.findDispatchSpec(
                manifest.tenantId(), manifest.jobId()).orElseThrow(() ->
                new IllegalStateException(
                        "published artifact has no immutable dispatch specification"))
                .stageGraph().stages().stream()
                .filter(stage -> stage.pageKind()
                        == org.pangrova.databuild.api.DataBuildPageKind.SOURCE_MAP)
                .findFirst().orElseThrow(() ->
                        new IllegalStateException("published source artifact has no source stage"))
                .bindingSetBytes();
        try {
            OperatorBindingSet set = OperatorBindingSet.parseFrom(bytes);
            if (!set.getUnknownFields().asMap().isEmpty()) {
                throw new IllegalStateException(
                        "published artifact dispatch binding set is not canonical");
            }
            List<OperatorBinding> outputBindings = set.getBindingsList().stream()
                    .filter(OperatorBinding::hasOutputArtifact)
                    .toList();
            if (outputBindings.size() != 1) {
                throw new IllegalStateException(
                        "published artifact dispatch must contain one output binding");
            }
            return outputBindings.get(0).getOutputArtifact();
        } catch (InvalidProtocolBufferException failure) {
            throw new IllegalStateException(
                    "published artifact dispatch binding set is malformed", failure);
        }
    }

    /** Query-scoped immutable metadata; physical objects remain task-scoped. */
    public record ResolvedRetrievalGeneration(
            RetrievalGenerationIntent intent, ArtifactBinding bindingTemplate,
            int sourceObjectCount) {
        public ResolvedRetrievalGeneration {
            Objects.requireNonNull(intent, "intent must not be null");
            Objects.requireNonNull(bindingTemplate,
                    "bindingTemplate must not be null");
            if (sourceObjectCount <= 0) {
                throw new IllegalArgumentException("resolved retrieval requires source objects");
            }
            if (bindingTemplate.getSplitObjectsCount() != 0
                    || bindingTemplate.hasBindingChecksum()) {
                throw new IllegalArgumentException(
                        "resolved template must not contain task-scoped objects or checksum");
            }
        }
    }

    /** Exact immutable identity requested by a feature-neutral artifact consumer. */
    public record GenerationResolutionIntent(
            String tenantId,
            String artifactId,
            String artifactType,
            String generation,
            String sourceId,
            String sourceSnapshot,
            SourceCoverage coverage) {

        public GenerationResolutionIntent {
            requireText(tenantId, "tenantId");
            requireText(artifactId, "artifactId");
            requireText(artifactType, "artifactType");
            requireText(generation, "generation");
            requireText(sourceId, "sourceId");
            requireText(sourceSnapshot, "sourceSnapshot");
            Objects.requireNonNull(coverage, "coverage");
        }

        private static void requireText(String value, String name) {
            if (value == null || value.isBlank()) {
                throw new IllegalArgumentException(name + " must not be blank");
            }
        }
    }

    /** Declares whether the consumer needs every published source or an exact checksummed query subset. */
    public enum SourceCoverage {
        COMPLETE_GENERATION,
        QUERY_SOURCE_SUBSET
    }
}
