package org.pangrova.coordinator.query.execution.dispatch.dag.retrieval;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import com.google.protobuf.InvalidProtocolBufferException;
import io.substrait.proto.NamedStruct;

import org.pangrova.common.exception.PangrovaErrorCode;
import org.pangrova.common.exception.PangrovaException;
import org.pangrova.coordinator.artifact.PublishedArtifactBindingResolver.ResolvedRetrievalArtifact;
import org.pangrova.planner.RetrievalTopKPlanHint;
import org.pangrova.planner.SerializedPlanFragment;
import org.pangrova.protocol.BoundExecutionPlan;
import org.pangrova.protocol.BuiltInCapabilityCatalog;
import org.pangrova.protocol.retrieval.RerankBinding;
import org.pangrova.protocol.retrieval.RerankPlanBinder;
import org.pangrova.protocol.retrieval.RetrievalFanOutBinding;
import org.pangrova.protocol.retrieval.RetrievalFanOutPlanBinder;
import org.pangrova.protocol.retrieval.RetrievalMergeBinding;
import org.pangrova.protocol.retrieval.RetrievalMergePlanBinder;
import org.pangrova.protocol.retrieval.RetrievalTopKBinding;
import org.pangrova.protocol.retrieval.RetrievalTopKPlanBinder;
import org.pangrova.protocol.v2.ScoreDirection;

/** Binds planner retrieval intents to canonical protocol operator bindings. */
public final class RetrievalPlanBinder {
    private static final double GRAPH_RECALL = 0.95;
    private static final double QUANTIZED_RECALL = 0.90;

    private RetrievalPlanBinder() {}

    /** Binds stage-local retrieval operators. */
    public static BoundExecutionPlan bindTypedCapabilities(
            SerializedPlanFragment fragment, String queryId, int stageId,
            Map<String, ResolvedRetrievalArtifact> retrievalArtifacts) {
        List<RetrievalTopKBinding> bindings = fragment.retrievalTopKHints().stream()
                .map(hint -> toStageBinding(hint, retrievalArtifacts))
                .toList();
        return RetrievalTopKPlanBinder.bindExecution(
                fragment.planBytes(), bindings, stagePrefix(queryId, stageId));
    }

    /** Binds a distributed fan-out, merge, and optional rerank stage. */
    public static BoundExecutionPlan bindDistributed(
            DistributedRetrieval request) {
        requireOperations(request);
        RetrievalTopKPlanHint hint = requireSingleHint(request);
        if (!request.outputStage() && request.mergeIntent().isEmpty()) {
            return bindTypedCapabilities(
                    request.fragment(), request.queryId(), request.stageId(),
                    request.artifacts());
        }
        requireValidOutputStage(request);
        if (request.outputStage() && request.mergeIntent().isEmpty()) {
            return bindRerankOnly(request, hint);
        }
        return bindMerge(request, hint, request.mergeIntent().orElseThrow());
    }

    /** Returns whether this stage needs the distributed retrieval contract. */
    public static boolean shouldBindDistributed(
            SerializedPlanFragment fragment,
            boolean outputStage,
            boolean hasNoSplits) {
        if (fragment.retrievalTopKHints().isEmpty()) {
            return false;
        }
        if (outputStage) {
            if (!hasNoSplits) {
                throw validationFailure(
                        "Distributed retrieval output must be a split-free "
                                + "singleton Native stage");
            }
            return true;
        }
        return fragment.retrievalTopKHints().stream().anyMatch(
                hint -> hint.kind() != RetrievalTopKPlanHint.Kind.EXACT);
    }

    private static BoundExecutionPlan bindMerge(
            DistributedRetrieval request,
            RetrievalTopKPlanHint hint,
            RetrievalMergeIntent merge) {
        RetrievalMergeBinding mergeBinding = toMergeBinding(hint, merge);
        if (request.outputStage() && request.hasNoSplits()) {
            return bindFinalMerge(request, hint, merge, mergeBinding);
        }
        List<RetrievalFanOutBinding.Source> sources = merge.sources().stream()
                .map(source -> new RetrievalFanOutBinding.Source(
                        source.sourceId(),
                        toFanOutBinding(hint, source, request.artifacts())))
                .toList();
        return RetrievalFanOutPlanBinder.bindExecution(
                request.fragment().planBytes(),
                new RetrievalFanOutBinding(sources),
                request.idPrefix());
    }

    private static BoundExecutionPlan bindFinalMerge(
            DistributedRetrieval request,
            RetrievalTopKPlanHint hint,
            RetrievalMergeIntent merge,
            RetrievalMergeBinding mergeBinding) {
        int finalLimit = request.rerankIntent().map(RerankIntent::outputLimit)
                .orElse(merge.outputLimit());
        BoundExecutionPlan merged = RetrievalMergePlanBinder.bindFinalTopKExecution(
                request.fragment().planBytes(), mergeBinding, finalLimit,
                request.idPrefix() + "-merge");
        if (request.rerankIntent().isEmpty()) {
            return merged;
        }
        BoundExecutionPlan reranked = RerankPlanBinder.bindExecution(
                merged.planBytes(),
                toRerankBinding(hint, request.rerankIntent().orElseThrow()),
                request.idPrefix() + "-rerank");
        List<org.pangrova.protocol.v2.OperatorBinding> operators =
                new ArrayList<>(merged.operatorBindings());
        operators.addAll(reranked.operatorBindings());
        return new BoundExecutionPlan(reranked.planBytes(), operators);
    }

    private static BoundExecutionPlan bindRerankOnly(
            DistributedRetrieval request, RetrievalTopKPlanHint hint) {
        RerankBinding rerank = toRerankBinding(
                hint, request.rerankIntent().orElseThrow());
        return RerankPlanBinder.bindFinalTopKExecution(
                request.fragment().planBytes(), rerank,
                request.idPrefix() + "-rerank");
    }

    private static RetrievalMergeBinding toMergeBinding(
            RetrievalTopKPlanHint hint, RetrievalMergeIntent merge) {
        RetrievalTopKBinding representative = toProtocolBinding(
                hint, toScoreDirection(hint), Optional.empty(),
                hint.candidateLimit());
        NamedStruct candidateSchema =
                RetrievalFanOutPlanBinder.candidateSchema(representative);
        int sourceIdField = candidateSchema.getStruct().getTypesCount() - 1;
        return new RetrievalMergeBinding(
                merge.strategy() == RetrievalMergeIntent.Strategy.RRF
                        ? RetrievalMergeBinding.Strategy.RRF
                        : RetrievalMergeBinding.Strategy.LINEAR,
                hint.scoreFieldIndex(), sourceIdField,
                hint.stableKeyFieldIndexes(), merge.outputLimit(),
                merge.sources().size(), merge.rrfK(),
                merge.sources().stream().map(source ->
                        new RetrievalMergeBinding.Source(
                                source.sourceId(), source.scoreDirection(),
                                source.weight(), source.candidateLimit())).toList(),
                candidateSchema.toByteArray());
    }

    private static RetrievalTopKPlanHint requireSingleHint(
            DistributedRetrieval request) {
        List<RetrievalTopKPlanHint> hints = request.outputStage()
                && request.hasNoSplits()
                ? request.fragment().retrievalTopKHints()
                : request.fragment().retrievalTopKHints().stream()
                        .filter(hint -> hint.kind()
                                != RetrievalTopKPlanHint.Kind.EXACT)
                        .toList();
        if (hints.size() != 1) {
            throw validationFailure(
                    "Distributed retrieval requires one canonical Top-K shape per stage");
        }
        return hints.get(0);
    }

    private static void requireOperations(DistributedRetrieval request) {
        if (request.mergeIntent().isEmpty() && request.rerankIntent().isEmpty()) {
            throw new IllegalArgumentException(
                    "distributed retrieval pipeline has no operations");
        }
    }

    private static void requireValidOutputStage(DistributedRetrieval request) {
        if (request.outputStage() && !request.hasNoSplits()) {
            throw validationFailure(
                    "Distributed retrieval requires a separate complete singleton "
                            + "Native output stage");
        }
    }

    private static RetrievalTopKBinding toStageBinding(
            RetrievalTopKPlanHint hint,
            Map<String, ResolvedRetrievalArtifact> artifacts) {
        ResolvedRetrievalArtifact artifact = artifacts.get(hint.sourceKey());
        int candidateLimit = artifact == null
                ? hint.candidateLimit() : artifact.intent().candidateLimit();
        return toProtocolBinding(
                hint, toScoreDirection(hint), Optional.ofNullable(artifact),
                candidateLimit);
    }

    private static RetrievalTopKBinding toFanOutBinding(
            RetrievalTopKPlanHint hint,
            RetrievalMergeIntent.Source source,
            Map<String, ResolvedRetrievalArtifact> artifacts) {
        ResolvedRetrievalArtifact artifact = artifacts.get(source.sourceId());
        requireFullTextArtifact(source, artifact);
        requireCapabilityKind(source, artifact);
        return toProtocolBinding(
                hint, source.scoreDirection(), Optional.ofNullable(artifact),
                source.candidateLimit());
    }

    private static void requireFullTextArtifact(
            RetrievalMergeIntent.Source source,
            ResolvedRetrievalArtifact artifact) {
        if (artifact == null
                && source.kind() == RetrievalMergeIntent.SourceKind.FULL_TEXT) {
            throw new PangrovaException(
                    PangrovaErrorCode.RUNTIME_TASK_FAILED,
                    "BM25 retrieval has no immutable artifact binding: "
                            + source.sourceId());
        }
    }

    private static void requireCapabilityKind(
            RetrievalMergeIntent.Source source,
            ResolvedRetrievalArtifact artifact) {
        if (artifact == null) {
            return;
        }
        String capabilityId = artifact.intent().capability().getCapabilityId();
        boolean matches = switch (source.kind()) {
            case VECTOR -> isVectorRetrievalCapability(capabilityId);
            case FULL_TEXT -> BuiltInCapabilityCatalog.BM25_RETRIEVAL_ID.equals(
                    capabilityId);
        };
        if (!matches) {
            throw validationFailure(
                    "Retrieval artifact capability does not match source: "
                            + source.sourceId());
        }
    }

    private static RetrievalTopKBinding toProtocolBinding(
            RetrievalTopKPlanHint hint,
            ScoreDirection direction,
            Optional<ResolvedRetrievalArtifact> retrievalArtifact,
            int candidateLimit) {
        if (candidateLimit < hint.candidateLimit()) {
            throw validationFailure(
                    "Retrieval source candidate limit cannot be below output Top-K");
        }
        if (retrievalArtifact.isEmpty()) {
            return RetrievalTopKBinding.exact(
                    hint.scoreFieldIndex(), candidateLimit, direction,
                    hint.stableKeyFieldIndexes(), hint.candidateSchema());
        }
        ResolvedRetrievalArtifact artifact = retrievalArtifact.orElseThrow();
        RetrievalProfile profile = retrievalProfile(
                artifact.intent().capability().getCapabilityId(), candidateLimit);
        return RetrievalTopKBinding.artifact(
                hint.scoreFieldIndex(), candidateLimit, direction,
                hint.stableKeyFieldIndexes(), hint.candidateSchema(),
                new RetrievalTopKBinding.ArtifactInput(
                        artifact.intent().sourceId(), artifact.intent().capability(),
                        artifact.intent().invocationOptions(),
                        artifact.bindingTemplate(), profile.profileId(),
                        profile.exactRescoreLimit(), profile.requiredRecallAtK(),
                        profile.recallK()));
    }

    private static RerankBinding toRerankBinding(
            RetrievalTopKPlanHint hint, RerankIntent intent) {
        NamedStruct schema = parseCandidateSchema(hint);
        List<Integer> documentFields = java.util.stream.IntStream
                .range(0, schema.getNamesCount())
                .filter(index -> schema.getNames(index).equalsIgnoreCase(
                        intent.documentField()))
                .boxed()
                .toList();
        if (documentFields.size() != 1) {
            throw validationFailure(
                    "Cross-Encoder document column '" + intent.documentField()
                            + "' must occur exactly once in the bounded candidate schema");
        }
        return new RerankBinding(
                hint.scoreFieldIndex(), documentFields.get(0),
                hint.stableKeyFieldIndexes(), intent.inputLimit(),
                intent.outputLimit(), intent.modelId(), intent.modelVersion(),
                intent.modelSignature(), intent.modelOutputType(),
                intent.modelLimits(), intent.query(),
                hint.candidateSchema());
    }

    private static NamedStruct parseCandidateSchema(RetrievalTopKPlanHint hint) {
        try {
            return NamedStruct.parseFrom(hint.candidateSchema());
        } catch (InvalidProtocolBufferException failure) {
            throw new PangrovaException(
                    PangrovaErrorCode.PLANNER_VALIDATION_FAILURE,
                    "Rerank candidate schema is malformed", failure);
        }
    }

    private static ScoreDirection toScoreDirection(RetrievalTopKPlanHint hint) {
        return switch (hint.scoreDirection()) {
            case LOWER_IS_BETTER ->
                    ScoreDirection.SCORE_DIRECTION_LOWER_IS_BETTER;
            case HIGHER_IS_BETTER ->
                    ScoreDirection.SCORE_DIRECTION_HIGHER_IS_BETTER;
        };
    }

    private static RetrievalProfile retrievalProfile(
            String capabilityId, int candidateLimit) {
        if (BuiltInCapabilityCatalog.DISKANN_RETRIEVAL_ID.equals(capabilityId)) {
            return new RetrievalProfile(
                    "diskann-l2-exact-rescore", candidateLimit,
                    GRAPH_RECALL, candidateLimit);
        }
        if (BuiltInCapabilityCatalog.HNSW_RETRIEVAL_ID.equals(capabilityId)) {
            return new RetrievalProfile(
                    "hnsw-metric-exact-rescore", candidateLimit,
                    GRAPH_RECALL, candidateLimit);
        }
        if (BuiltInCapabilityCatalog.IVF_PQ_RETRIEVAL_ID.equals(capabilityId)) {
            return new RetrievalProfile(
                    "ivfpq-metric-exact-rescore", candidateLimit,
                    QUANTIZED_RECALL, candidateLimit);
        }
        if (BuiltInCapabilityCatalog.BM25_RETRIEVAL_ID.equals(capabilityId)) {
            return new RetrievalProfile(
                    "bm25-shard-exact", candidateLimit, 1.0, candidateLimit);
        }
        throw new PangrovaException(
                PangrovaErrorCode.RUNTIME_TASK_FAILED,
                "No retrieval execution profile for capability=" + capabilityId);
    }

    private static boolean isVectorRetrievalCapability(String capabilityId) {
        return BuiltInCapabilityCatalog.DISKANN_RETRIEVAL_ID.equals(capabilityId)
                || BuiltInCapabilityCatalog.HNSW_RETRIEVAL_ID.equals(capabilityId)
                || BuiltInCapabilityCatalog.IVF_PQ_RETRIEVAL_ID.equals(capabilityId);
    }

    private static String stagePrefix(String queryId, int stageId) {
        return queryId + "-stage-" + stageId;
    }

    private static PangrovaException validationFailure(String message) {
        return new PangrovaException(
                PangrovaErrorCode.PLANNER_VALIDATION_FAILURE, message);
    }

    /** Immutable distributed retrieval binding request. */
    public record DistributedRetrieval(
            SerializedPlanFragment fragment,
            String queryId,
            int stageId,
            boolean outputStage,
            boolean hasNoSplits,
            Optional<RetrievalMergeIntent> mergeIntent,
            Optional<RerankIntent> rerankIntent,
            Map<String, ResolvedRetrievalArtifact> artifacts) {

        String idPrefix() {
            return stagePrefix(queryId, stageId);
        }
    }

    private record RetrievalProfile(
            String profileId,
            int exactRescoreLimit,
            double requiredRecallAtK,
            int recallK) {}
}
