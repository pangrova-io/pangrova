package org.pangrova.coordinator.query.execution.dispatch.dag.retrieval;

import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import org.pangrova.common.exception.PangrovaErrorCode;
import org.pangrova.common.exception.PangrovaException;
import org.pangrova.coordinator.generation.PublishedGenerationBindingResolver;
import org.pangrova.coordinator.generation.PublishedGenerationBindingResolver.ResolvedRetrievalGeneration;
import org.pangrova.coordinator.query.execution.dispatch.dag.DAGQueryContext;
import org.pangrova.coordinator.query.execution.split.SplitFetcher;
import org.pangrova.planner.RetrievalTopKPlanHint;
import org.pangrova.planner.SerializedPlanFragment;
import org.pangrova.protocol.capability.BuiltInCapabilityCatalog;
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
            DAGQueryContext context,
            List<SplitFetcher.SplitWithPaths> splits) {
        if (!requiresResolution(fragment, context, splits)) {
            return Map.of();
        }
        Map<String, RetrievalGenerationIntent> intents = indexIntents(context);
        List<String> serializedSplits = splits.stream()
                .map(SplitFetcher.SplitWithPaths::serializedData)
                .toList();
        if (context.retrievalMerge().isPresent()) {
            return resolveMergeArtifacts(context, intents, serializedSplits);
        }
        return resolveStageArtifacts(fragment, intents, serializedSplits);
    }

    private static boolean requiresResolution(
            SerializedPlanFragment fragment,
            DAGQueryContext context,
            List<SplitFetcher.SplitWithPaths> splits) {
        return context != null && !splits.isEmpty()
                && fragment.retrievalTopKHints().stream().anyMatch(
                        hint -> hint.kind() != RetrievalTopKPlanHint.Kind.EXACT);
    }

    private static Map<String, RetrievalGenerationIntent> indexIntents(
            DAGQueryContext context) {
        Map<String, RetrievalGenerationIntent> intents = new LinkedHashMap<>();
        for (RetrievalGenerationIntent intent : context.retrievalArtifacts()) {
            if (intents.put(intent.planSourceKey(), intent) != null) {
                throw validationFailure(
                        "Duplicate retrieval source binding: "
                                + intent.planSourceKey());
            }
        }
        return intents;
    }

    private Map<String, ResolvedRetrievalGeneration> resolveMergeArtifacts(
            DAGQueryContext context,
            Map<String, RetrievalGenerationIntent> intents,
            List<String> serializedSplits) {
        Set<String> mergeSources = context.retrievalMerge().orElseThrow()
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
        for (RetrievalGenerationIntent intent : context.retrievalArtifacts()) {
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
        if (hint.kind() == RetrievalTopKPlanHint.Kind.EXACT) {
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
        Optional<ResolvedRetrievalGeneration> artifact =
                bindingResolver.resolveTemplate(intent, serializedSplits);
        if (artifact.isEmpty()) {
            requireBm25Artifact(intent);
            LOG.info("event=retrieval_artifact_exact_fallback artifactId={}"
                            + " reason=unpublished_or_snapshot_mismatch",
                    intent.artifactId());
        }
        return artifact;
    }

    private static void requireFullTextIntent(RetrievalTopKPlanHint hint) {
        if (hint.kind() == RetrievalTopKPlanHint.Kind.FULL_TEXT) {
            throw new PangrovaException(
                    PangrovaErrorCode.RUNTIME_TASK_FAILED,
                    "BM25 retrieval has no immutable artifact binding: "
                            + hint.sourceKey());
        }
    }

    private static void requireBm25Artifact(RetrievalGenerationIntent intent) {
        if (BuiltInCapabilityCatalog.BM25_RETRIEVAL_ID.equals(
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
