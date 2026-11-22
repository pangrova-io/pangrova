package org.pangrova.generation.publication.read;

import org.pangrova.generation.codec.source.GenerationSourceSplitValidator;

import org.pangrova.generation.publication.enums.GenerationSourceCoverage;

import com.google.protobuf.InvalidProtocolBufferException;
import org.pangrova.generation.api.output.GenerationOutputFormat;
import org.pangrova.generation.api.publication.GenerationManifest;
import org.pangrova.generation.api.output.GenerationOutput;
import org.pangrova.generation.api.page.GenerationSplit;
import org.pangrova.generation.api.enums.GenerationOutputScope;
import org.pangrova.generation.api.job.GenerationDispatchSpec;
import org.pangrova.persistence.generation.api.job.GenerationJobStore;
import org.pangrova.generation.api.graph.template.GenerationStageTemplate.PublicationRead;
import org.pangrova.persistence.generation.api.execution.GenerationStageExecutionStore.PublishedInput;
import org.pangrova.protocol.generation.PublishedGenerationBindingChecksum;
import org.pangrova.protocol.v2.ArtifactBinding;
import org.pangrova.protocol.v2.ArtifactWriteBinding;
import org.pangrova.protocol.v2.DataSplit;
import org.pangrova.protocol.v2.OperatorBinding;
import org.pangrova.protocol.v2.OperatorBindingSet;
import org.pangrova.security.api.authorization.DeferredAuthorizationScope.DefinitionSnapshot;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

import static org.pangrova.common.util.Preconditions.requireText;

/**
 * Resolves immutable artifact metadata on the Coordinator control plane.
 *
 * <p>The query-level resolution deliberately excludes physical objects. Those
 * are loaded for the assigned sources of a dispatch window. Each task receives
 * its own source objects and the publication's shared generation outputs.</p>
 */
public final class PublishedGenerationBindingResolver {

    private final GenerationJobStore store;

    public PublishedGenerationBindingResolver(GenerationJobStore store) {
        this.store = Objects.requireNonNull(store, "store must not be null");
    }

    /**
     * Resolves an object-free binding for any published immutable artifact.
     * Feature planners attach the returned generic binding to their own exact
     * capability; this layer has no vector/search/generated-value branching.
     */
    public Optional<ResolvedTemplate> resolveTemplate(
            GenerationResolutionIntent intent, List<DataSplit> splits) {
        Objects.requireNonNull(intent, "intent must not be null");
        splits.forEach(GenerationSourceSplitValidator::validateSplit);
        if (splits.isEmpty() || splits.stream().anyMatch(split ->
                !split.getSourceId().equals(intent.sourceId())
                        || !split.getSourceSnapshot().equals(intent.sourceSnapshot()))) {
            return Optional.empty();
        }
        Optional<Map<String, String>> splitIdentities = splitIdentities(splits);
        if (splitIdentities.isEmpty()) {
            return Optional.empty();
        }
        GenerationManifest manifest = store.findPublishedGeneration(
                intent.tenantId(), intent.artifactId(),
                intent.generation(), intent.sourceSnapshot())
                .orElse(null);
        if (manifest == null) {
            return Optional.empty();
        }
        PublishedGenerationBindingValidator.validateManifest(intent, manifest);
        GenerationOutputFormat contract = PublishedGenerationBindingValidator.requireFormatContract(
                manifest.formatId(), manifest.artifactType());
        long publishedSourceRoles = contract.sourceMapRoles().values().stream()
                .filter(role -> role.scope() == GenerationOutputScope.SOURCE_OBJECT)
                .count();
        long expectedObjects = Math.addExact(
                Math.multiplyExact((long) splitIdentities.orElseThrow().size(),
                        publishedSourceRoles),
                contract.generationFinalizeRoles().size());
        if (!matchesCoverage(intent.coverage(), manifest.objectCount(), expectedObjects)
                || !matchesPublishedSourceRanges(manifest, splitIdentities.orElseThrow())) {
            return Optional.empty();
        }
        var dispatch = store.findDispatchSpec(manifest.tenantId(), manifest.jobId()).orElseThrow(() ->
                new IllegalStateException("published artifact has no immutable dispatch specification"));
        ArtifactWriteBinding writeTemplate = loadWriteTemplate(dispatch);
        PublishedGenerationBindingValidator.validateWriteTemplate(manifest, writeTemplate);
        return Optional.of(new ResolvedTemplate(bindingTemplate(manifest, writeTemplate),
                manifest.rowCount(), manifest.totalBytes(), dispatch.authorizationScope().definitionSnapshot()));
    }

    /** Verified binding, resource totals and definition from the admitted publication and dispatch specification. */
    public record ResolvedTemplate(ArtifactBinding binding, long rowCount, long totalBytes,
                                   Optional<DefinitionSnapshot> definitionSnapshot) {}

    private static boolean matchesCoverage(GenerationSourceCoverage coverage, long published, long requested) {
        return coverage == GenerationSourceCoverage.COMPLETE_GENERATION ? published == requested
                : published >= requested;
    }

    private boolean matchesPublishedSourceRanges(
            GenerationManifest manifest, Map<String, String> requested) {
        Map<String, GenerationSplit> stored = store.loadPublishedSourceSplits(
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
            GenerationManifest manifest, ArtifactWriteBinding writeTemplate) {
        return ArtifactBinding.newBuilder()
                .setArtifactId(manifest.artifactId())
                .setArtifactType(manifest.artifactType())
                .setGeneration(manifest.generation())
                .setSourceId(manifest.sourceId())
                .setSourceSnapshot(manifest.sourceSnapshot())
                .setArtifactCreatedAtEpochMs(manifest.publishedAt().toEpochMilli())
                .setReadEpoch(manifest.manifestSha256())
                .setManifestUri(manifest.manifestUri())
                .setManifestChecksum(PublishedGenerationBindingValidator.sha256(
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

    /** Reads each publication once for the assigned sources in an exact live dispatch window. */
    public List<List<OperatorBinding>> materializeForTasks(
            String tenantId, List<OperatorBinding> bindings, List<List<DataSplit>> taskSplits) {
        requireText(tenantId, "tenantId");
        if (taskSplits.isEmpty()) return List.of();

        Map<PublicationRead, ArtifactBinding> templates = new LinkedHashMap<>();
        for (var binding : bindings) {
            for (var artifact : binding.getArtifactsList()) {
                templates.putIfAbsent(publicationRead(artifact), artifact);
            }
        }
        if (templates.isEmpty()) return java.util.Collections.nCopies(taskSplits.size(), List.copyOf(bindings));

        List<Map<PublicationRead, PublishedInput>> prepared = new ArrayList<>(taskSplits.size());
        for (var splits : taskSplits) {
            splits.forEach(GenerationSourceSplitValidator::validateSplit);
            prepared.add(new HashMap<>());
        }
        templates.forEach((read, template) -> loadWindow(tenantId, read, template, taskSplits, prepared));
        List<List<OperatorBinding>> result = new ArrayList<>(taskSplits.size());
        for (int partition = 0; partition < taskSplits.size(); partition++) {
            result.add(materializeValidated(bindings, taskSplits.get(partition), prepared.get(partition)));
        }
        return List.copyOf(result);
    }

    private void loadWindow(String tenantId, PublicationRead read, ArtifactBinding template,
            List<List<DataSplit>> taskSplits, List<Map<PublicationRead, PublishedInput>> prepared) {
        var taskSources = taskSplits.stream().map(splits -> matchingSourceObjects(template, splits)).toList();
        Set<String> windowSources = new HashSet<>();
        taskSources.forEach(windowSources::addAll);
        GenerationManifest manifest = store.findPublishedGeneration(
                tenantId, read.artifactId(), read.generation(), read.sourceSnapshot())
                .orElseThrow(() -> new IllegalStateException("pinned artifact generation is no longer published"));
        List<GenerationOutput> generationOutputs = new ArrayList<>();
        Map<String, List<GenerationOutput>> sourceOutputs = new HashMap<>();
        for (var output : store.loadPublishedOutputs(manifest.tenantId(), manifest.jobId(), windowSources)) {
            if (output.scope() == GenerationOutputScope.GENERATION) {
                generationOutputs.add(output);
            } else if (output.scope() == GenerationOutputScope.SOURCE_OBJECT
                    && windowSources.contains(output.sourceObjectId())) {
                sourceOutputs.computeIfAbsent(output.sourceObjectId(), ignored -> new ArrayList<>()).add(output);
            } else {
                throw new IllegalStateException("artifact object lookup returned an unexpected scope");
            }
        }
        for (int partition = 0; partition < taskSources.size(); partition++) {
            var sourceIds = taskSources.get(partition);
            List<GenerationOutput> outputs = new ArrayList<>(generationOutputs);
            for (String sourceId : sourceIds) {
                outputs.addAll(sourceOutputs.getOrDefault(sourceId, List.of()));
            }
            prepared.get(partition).put(read, new PublishedInput(manifest, outputs, sourceIds));
        }
    }

    /** Pure task-local materialization from immutable evidence read on the claim transaction's connection. */
    public static List<OperatorBinding> materializePrepared(
            List<OperatorBinding> bindings, List<DataSplit> splits,
            Map<PublicationRead, PublishedInput> publications) {
        splits.forEach(GenerationSourceSplitValidator::validateSplit);
        return materializeValidated(bindings, splits, publications);
    }

    private static List<OperatorBinding> materializeValidated(
            List<OperatorBinding> bindings, List<DataSplit> splits,
            Map<PublicationRead, PublishedInput> publications) {
        Set<PublicationRead> required = bindings.stream().flatMap(binding -> binding.getArtifactsList().stream())
                .map(PublishedGenerationBindingResolver::publicationRead).collect(java.util.stream.Collectors.toSet());
        if (!required.equals(publications.keySet())) {
            throw new IllegalStateException(
                    "Prepared publication evidence differs from immutable stage read templates");
        }
        return bindings.stream().map(binding -> {
            if (binding.getArtifactsCount() == 0) return binding;
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
        PublishedGenerationBindingValidator.validateTemplateAgainstManifest(template, evidence.manifest());
        List<GenerationOutput> outputs = evidence.outputs();
        PublishedGenerationBindingValidator.validateObjectCoverage(
                template, sourceObjectIds, outputs);

        ArtifactBinding.Builder builder = template.toBuilder();
        outputs.stream()
                .sorted(java.util.Comparator
                        .comparing(GenerationOutput::sourceObjectId)
                        .thenComparingInt(GenerationOutput::ordinal)
                        .thenComparing(GenerationOutput::objectId))
                .map(PublishedGenerationBindingValidator::toProtocolObject)
                .forEach(builder::addSplitObjects);
        return PublishedGenerationBindingChecksum.sign(builder.build());
    }

    private static ArtifactWriteBinding loadWriteTemplate(GenerationDispatchSpec dispatch) {
        byte[] bytes = dispatch.stageGraph().stages().stream()
                .filter(stage -> stage.pageKind()
                        == org.pangrova.generation.api.enums.GenerationPageKind.SOURCE_MAP)
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

    /** Exact immutable identity requested by a feature-neutral artifact consumer. */
    public record GenerationResolutionIntent(
            String tenantId,
            String artifactId,
            String artifactType,
            String generation,
            String sourceId,
            String sourceSnapshot,
            GenerationSourceCoverage coverage) {

        public GenerationResolutionIntent {
            requireText(tenantId, "tenantId");
            requireText(artifactId, "artifactId");
            requireText(artifactType, "artifactType");
            requireText(generation, "generation");
            requireText(sourceId, "sourceId");
            requireText(sourceSnapshot, "sourceSnapshot");
            Objects.requireNonNull(coverage, "coverage");
        }
    }
}
