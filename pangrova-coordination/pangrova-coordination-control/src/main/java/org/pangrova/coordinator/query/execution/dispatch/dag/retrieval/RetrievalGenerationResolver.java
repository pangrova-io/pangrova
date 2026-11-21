package org.pangrova.coordinator.query.execution.dispatch.dag.retrieval;

import org.pangrova.planner.api.enums.RetrievalKind;
import org.pangrova.protocol.constants.BuiltInCapabilityIdentifiers;
import org.pangrova.generation.publication.enums.GenerationSourceCoverage;
import org.pangrova.execution.stage.binding.RetrievalGenerationIntent;
import org.pangrova.execution.stage.binding.RetrievalMergeIntent;
import org.pangrova.query.api.task.QuerySplit;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import org.pangrova.common.enums.PangrovaErrorCode;
import org.pangrova.common.exception.PangrovaException;
import org.pangrova.generation.publication.read.PublishedGenerationBindingResolver;
import org.pangrova.planner.api.retrieval.RetrievalTopKPlanHint;
import org.pangrova.planner.api.fragment.SerializedPlanFragment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Resolves immutable retrieval artifacts against the query snapshot. */
public final class RetrievalGenerationResolver {
    private static final Logger LOG =
            LoggerFactory.getLogger(RetrievalGenerationResolver.class);

    private final PublishedGenerationBindingResolver bindingResolver;

    /** Creates a resolver; a null binding resolver permits exact vector fallback. */
    public RetrievalGenerationResolver(
            PublishedGenerationBindingResolver bindingResolver) {
        this.bindingResolver = bindingResolver;
    }

    /** Resolves all retrieval artifact intents consumed by a physical stage. */
    public Map<String, ResolvedRetrievalGeneration> resolve(
            SerializedPlanFragment fragment,
            List<RetrievalGenerationIntent> artifacts, Optional<RetrievalMergeIntent> merge,
            List<QuerySplit> splits) {
        if (!requiresResolution(fragment, splits)) {
            return Map.of();
        }
        Map<String, RetrievalGenerationIntent> intents = indexIntents(artifacts);
        List<String> serializedSplits = splits.stream()
                .map(QuerySplit::serializedData)
                .toList();
        if (merge.isPresent()) {
            return resolveMergeArtifacts(artifacts, merge, intents, serializedSplits);
        }
        return resolveStageArtifacts(fragment, intents, serializedSplits);
    }

    private static boolean requiresResolution(
            SerializedPlanFragment fragment,
            List<QuerySplit> splits) {
        return !splits.isEmpty()
                && fragment.retrievalTopKHints().stream().anyMatch(
                        hint -> hint.kind() != RetrievalKind.EXACT);
    }

    private static Map<String, RetrievalGenerationIntent> indexIntents(
            List<RetrievalGenerationIntent> artifacts) {
        Map<String, RetrievalGenerationIntent> intents = new LinkedHashMap<>();
        for (RetrievalGenerationIntent intent : artifacts) {
            if (intents.put(intent.planSourceKey(), intent) != null) {
                throw validationFailure(
                        "Duplicate retrieval source binding: "
                                + intent.planSourceKey());
            }
        }
        return intents;
    }

    private Map<String, ResolvedRetrievalGeneration> resolveMergeArtifacts(
            List<RetrievalGenerationIntent> artifacts, Optional<RetrievalMergeIntent> merge,
            Map<String, RetrievalGenerationIntent> intents,
            List<String> serializedSplits) {
        Set<String> mergeSources = merge.orElseThrow()
                .sources().stream()
                .map(RetrievalMergeIntent.Source::sourceId)
                .collect(Collectors.toUnmodifiableSet());
        List<String> unexpected = intents.keySet().stream()
                .filter(key -> !mergeSources.contains(key))
                .toList();
        if (!unexpected.isEmpty()) {
            throw validationFailure(
                    "Artifact intents are absent from the retrieval merge contract: "
                            + unexpected);
        }
        Map<String, ResolvedRetrievalGeneration> resolved = new LinkedHashMap<>();
        for (RetrievalGenerationIntent intent : artifacts) {
            resolveOne(intent, serializedSplits)
                    .ifPresent(artifact -> resolved.put(
                            intent.planSourceKey(), artifact));
        }
        return Map.copyOf(resolved);
    }

    private Map<String, ResolvedRetrievalGeneration> resolveStageArtifacts(
            SerializedPlanFragment fragment,
            Map<String, RetrievalGenerationIntent> intents,
            List<String> serializedSplits) {
        Map<String, ResolvedRetrievalGeneration> resolved = new LinkedHashMap<>();
        Set<String> consumed = new HashSet<>();
        for (RetrievalTopKPlanHint hint : fragment.retrievalTopKHints()) {
            resolveHint(hint, intents, serializedSplits, consumed)
                    .ifPresent(artifact -> resolved.put(hint.sourceKey(), artifact));
        }
        List<String> unconsumed = intents.keySet().stream()
                .filter(key -> !consumed.contains(key))
                .toList();
        if (!unconsumed.isEmpty()) {
            throw validationFailure(
                    "Retrieval sources are not represented in the physical Native stage: "
                            + unconsumed);
        }
        return Map.copyOf(resolved);
    }

    private Optional<ResolvedRetrievalGeneration> resolveHint(
            RetrievalTopKPlanHint hint,
            Map<String, RetrievalGenerationIntent> intents,
            List<String> serializedSplits,
            Set<String> consumed) {
        if (hint.kind() == RetrievalKind.EXACT) {
            return Optional.empty();
        }
        RetrievalGenerationIntent intent = intents.get(hint.sourceKey());
        if (intent == null) {
            requireFullTextIntent(hint);
            return Optional.empty();
        }
        consumed.add(intent.planSourceKey());
        return resolveOne(intent, serializedSplits);
    }

    private Optional<ResolvedRetrievalGeneration> resolveOne(
            RetrievalGenerationIntent intent, List<String> serializedSplits) {
        if (bindingResolver == null) {
            requireBm25Artifact(intent);
            return Optional.empty();
        }
        Optional<ResolvedRetrievalGeneration> artifact = resolvePublished(intent, serializedSplits);
        if (artifact.isEmpty()) {
            requireBm25Artifact(intent);
            LOG.info("event=retrieval_artifact_exact_fallback artifactId={}"
                            + " reason=unpublished_or_snapshot_mismatch",
                    intent.artifactId());
        }
        return artifact;
    }

    private Optional<ResolvedRetrievalGeneration> resolvePublished(
            RetrievalGenerationIntent intent, List<String> serializedSplits) {
        if (BuiltInCapabilityIdentifiers.FILTERED_DISKANN_RETRIEVAL_ID.equals(intent.capability().getCapabilityId())
                && !FilteredAnnSourceScope.supports(
                        org.pangrova.generation.codec.source.GenerationSourceSplitCodec.decode(serializedSplits),
                        intent.sourceId(), intent.sourceSnapshot())) {
            return Optional.empty();
        }
        return bindingResolver.resolveTemplate(
                new PublishedGenerationBindingResolver.GenerationResolutionIntent(
                        intent.tenantId(), intent.artifactId(), intent.artifactType(), intent.generation(),
                        intent.sourceId(), intent.sourceSnapshot(),
                        GenerationSourceCoverage.COMPLETE_GENERATION), serializedSplits)
                .map(binding -> new ResolvedRetrievalGeneration(intent, binding, serializedSplits.size()));
    }

    private static void requireFullTextIntent(RetrievalTopKPlanHint hint) {
        if (hint.kind() == RetrievalKind.FULL_TEXT) {
            throw new PangrovaException(
                    PangrovaErrorCode.RUNTIME_TASK_FAILED,
                    "BM25 retrieval has no immutable artifact binding: "
                            + hint.sourceKey());
        }
    }

    private static void requireBm25Artifact(RetrievalGenerationIntent intent) {
        if (BuiltInCapabilityIdentifiers.BM25_RETRIEVAL_ID.equals(
                intent.capability().getCapabilityId())) {
            throw new PangrovaException(
                    PangrovaErrorCode.RUNTIME_TASK_FAILED,
                    "BM25_SCORE requires a complete immutable BM25 artifact "
                            + "at the query snapshot: " + intent.artifactId());
        }
    }

    private static PangrovaException validationFailure(String message) {
        return new PangrovaException(
                PangrovaErrorCode.PLANNER_VALIDATION_FAILURE, message);
    }
}
