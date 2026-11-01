package org.pangrova.coordinator.query.execution.retrieval;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.List;
import java.util.Map;

import com.google.protobuf.ByteString;
import org.pangrova.common.exception.PangrovaErrorCode;
import org.pangrova.common.exception.PangrovaException;
import org.pangrova.coordinator.query.execution.DispatchQueryContext;
import org.pangrova.coordinator.query.execution.dispatch.dag.retrieval.RerankIntent;
import org.pangrova.coordinator.query.execution.dispatch.dag.retrieval.RetrievalArtifactIntent;
import org.pangrova.coordinator.query.execution.dispatch.dag.retrieval.RetrievalMergeIntent;
import org.pangrova.coordinator.worker.WorkerCapabilityMatcher;
import org.pangrova.coordinator.worker.WorkerManager;
import org.pangrova.indexing.api.ArtifactTypeIds;
import org.pangrova.planner.rerank.RerankerPlanNode;
import org.pangrova.protocol.BuiltInCapabilityCatalog;
import org.pangrova.protocol.retrieval.RetrievalMergeBinding;
import org.pangrova.protocol.v2.CapabilityRef;
import org.pangrova.protocol.v2.LogicalType;
import org.pangrova.protocol.v2.NamedValue;
import org.pangrova.protocol.v2.ScoreDirection;
import org.pangrova.protocol.v2.TensorValue;
import org.pangrova.protocol.v2.TypedValue;

/** Binds validated retrieval/rerank intent for Native task dispatch. */
public final class RetrievalIntentCollector {
    private static final int DEFAULT_RRF_RANK_CONSTANT = 60;
    private static final double DEFAULT_BM25_K1 = 1.2;
    private static final double DEFAULT_BM25_B = 0.75;

    private final WorkerManager workerManager;

    /** Creates a collector using the active Worker inventory for exact reranker binding. */
    public RetrievalIntentCollector(WorkerManager workerManager) {
        this.workerManager = workerManager;
    }

    /** Builds immutable artifact intents for indexed vector and full-text sources. */
    public List<RetrievalArtifactIntent> collectRetrievalArtifactIntents(
            DispatchQueryContext ctx, String tenantId) {
        Map<String, RetrievalArtifactIntent> intents = new java.util.LinkedHashMap<>();
        ctx.vectorSearchNode().ifPresent(node -> {
            if (node.indexId() != null && !node.indexId().isBlank()
                    && node.indexGeneration() >= 0
                    && !node.indexSnapshot().isBlank()
                    && (node.serializedPreFilters().isEmpty()
                        || node.retrievalCapability().getCapabilityId().equals(
                                BuiltInCapabilityCatalog.FILTERED_DISKANN_RETRIEVAL_ID))) {
                int candidateLimit = vectorCandidateLimit(ctx, node);
                String sourceKey = "VECTOR:" + node.vectorColumn();
                intents.put(sourceKey, new RetrievalArtifactIntent(
                        tenantId,
                        sourceKey,
                        node.indexId(), ArtifactTypeIds.VECTOR_INDEX,
                        Integer.toString(node.indexGeneration()), node.tableName(),
                        node.indexSnapshot(),
                        node.retrievalCapability(),
                        vectorInvocationOptions(
                                node.queryVector(), node.retrievalCapability(),
                                node.searchBudget(), node.metric()),
                        candidateLimit));
            }
        });
        ctx.rerankerPlan().ifPresent(plan -> plan.sources().stream()
                .filter(org.pangrova.planner.rerank.RerankerPlanNode.FullTextSource.class::isInstance)
                .map(org.pangrova.planner.rerank.RerankerPlanNode.FullTextSource.class::cast)
                .map(org.pangrova.planner.rerank.RerankerPlanNode.FullTextSource::scoringNode)
                .filter(node -> !node.indexId().isBlank()
                        && node.indexGeneration() >= 0
                        && !node.indexSnapshot().isBlank()
                        && node.serializedPreFilters().isEmpty())
                .forEach(node -> {
                    String sourceKey = "FULL_TEXT:" + node.column();
                    intents.put(sourceKey, new RetrievalArtifactIntent(
                            tenantId,
                            sourceKey,
                            node.indexId(), ArtifactTypeIds.TEXT_INDEX,
                            Integer.toString(node.indexGeneration()), node.tableName(),
                            node.indexSnapshot(),
                            BuiltInCapabilityCatalog.bm25RetrievalPlanRef(),
                            bm25InvocationOptions(node.queryText()),
                            fullTextCandidateLimit(ctx, node.column())));
                }));
        return List.copyOf(intents.values());
    }

    /** Builds the global Native merge contract for a hybrid retrieval plan. */
    public java.util.Optional<RetrievalMergeIntent>
            collectRetrievalMergeIntent(DispatchQueryContext ctx) {
        if (ctx.rerankerPlan().isEmpty()
                || !ctx.rerankerPlan().orElseThrow().isHybrid()) {
            return java.util.Optional.empty();
        }
        RerankerPlanNode plan = ctx.rerankerPlan().orElseThrow();
        if (plan.isMultiVector()) {
            throw new PangrovaException(
                    PangrovaErrorCode.PLANNER_VALIDATION_FAILURE,
                    "Multi-vector retrieval requires per-source expression bindings; "
                            + "reusing one vector expression across sources is forbidden");
        }
        RetrievalMergeIntent.Strategy strategy;
        long rrfK = 0;
        double[] weights = new double[plan.sources().size()];
        java.util.Arrays.fill(weights, 1.0);
        RerankerPlanNode.RerankerStrategy fusion = fusionStrategy(plan);
        if (fusion instanceof RerankerPlanNode.RrfStrategy rrf) {
            strategy = RetrievalMergeIntent.Strategy.RRF;
            rrfK = rrf.k();
        } else if (fusion
                instanceof RerankerPlanNode.LinearCombinationStrategy linear) {
            if (plan.sources().size() != 2) {
                throw new PangrovaException(
                        PangrovaErrorCode.PLANNER_VALIDATION_FAILURE,
                        "Linear retrieval fusion requires exactly two sources");
            }
            strategy = RetrievalMergeIntent.Strategy.LINEAR;
            weights[0] = linear.weight1();
            weights[1] = linear.weight2();
        } else {
            throw new IllegalStateException(
                    "validated hybrid pipeline has no fusion strategy");
        }
        List<RetrievalMergeIntent.Source> sources = new java.util.ArrayList<>();
        for (int index = 0; index < plan.sources().size(); ++index) {
            RerankerPlanNode.RetrievalSource source = plan.sources().get(index);
            int candidateLimit = hybridCandidateLimit(ctx, plan, source);
            if (source instanceof RerankerPlanNode.VectorSource vector) {
                sources.add(new RetrievalMergeIntent.Source(
                        "VECTOR:" + vector.column(),
                        RetrievalMergeIntent.SourceKind.VECTOR,
                        ScoreDirection.SCORE_DIRECTION_LOWER_IS_BETTER,
                        weights[index], candidateLimit));
            } else if (source instanceof RerankerPlanNode.FullTextSource text) {
                sources.add(new RetrievalMergeIntent.Source(
                        "FULL_TEXT:" + text.scoringNode().column(),
                        RetrievalMergeIntent.SourceKind.FULL_TEXT,
                        ScoreDirection.SCORE_DIRECTION_HIGHER_IS_BETTER,
                        weights[index], candidateLimit));
            }
        }
        return java.util.Optional.of(new RetrievalMergeIntent(
                strategy, preRerankLimit(plan), rrfK, sources));
    }

    /** Resolves and fences an optional Cross-Encoder model for Native reranking. */
    public java.util.Optional<RerankIntent> collectRerankIntent(
            DispatchQueryContext ctx) {
        if (ctx.rerankerPlan().isEmpty()) {
            return java.util.Optional.empty();
        }
        RerankerPlanNode plan = ctx.rerankerPlan().orElseThrow();
        java.util.Optional<RerankerPlanNode.CrossEncoderStrategy> cross =
                crossEncoderStrategy(plan);
        if (cross.isEmpty()) {
            return java.util.Optional.empty();
        }
        if (plan.isMultiVector()) {
            throw new PangrovaException(
                    PangrovaErrorCode.PLANNER_VALIDATION_FAILURE,
                    "Cross-Encoder does not accept an ambiguous multi-vector source");
        }
        if (plan.strategy() instanceof RerankerPlanNode.CascadeStrategy
                && !plan.isHybrid()) {
            throw new PangrovaException(
                    PangrovaErrorCode.PLANNER_VALIDATION_FAILURE,
                    "Rerank cascade requires at least two retrieval sources");
        }
        var textSources = plan.sources().stream()
                .filter(RerankerPlanNode.FullTextSource.class::isInstance)
                .map(RerankerPlanNode.FullTextSource.class::cast)
                .toList();
        if (plan.queryText() == null || plan.queryText().isBlank()
                || textSources.size() != 1
                || !plan.queryText().equals(
                        textSources.get(0).scoringNode().queryText())) {
            throw new PangrovaException(
                    PangrovaErrorCode.PLANNER_VALIDATION_FAILURE,
                    "Cross-Encoder requires exactly one MATCH/BM25 text source "
                            + "to bind an unambiguous query and document column");
        }
        int inputLimit = cross.orElseThrow().topN();
        if (inputLimit < plan.topK()) {
            throw new PangrovaException(
                    PangrovaErrorCode.PLANNER_VALIDATION_FAILURE,
                    "Cross-Encoder top_n must be greater than or equal to the "
                            + "final query LIMIT");
        }
        org.pangrova.protocol.v2.ModelAvailability availability = resolveRerankModel(
                cross.orElseThrow().modelName());
        org.pangrova.protocol.v2.VersionedRef model = availability.getModel();
        org.pangrova.protocol.v2.CapabilityLimits limits = availability.getLimits();
        LogicalType outputType = availability.getCapabilityContractsList().stream()
                .filter(contract -> WorkerCapabilityMatcher.matchesCapabilityRef(
                        contract.getCapability(),
                        BuiltInCapabilityCatalog.crossEncoderRerankPlanRef()))
                .map(org.pangrova.protocol.v2.ModelCapabilityContract::getOutputType)
                .findFirst()
                .orElseThrow(() -> new PangrovaException(
                        PangrovaErrorCode.PLANNER_VALIDATION_FAILURE,
                        "Resolved Cross-Encoder model has no exact output contract"));
        return java.util.Optional.of(new RerankIntent(
                inputLimit,
                plan.topK(),
                model.getId(),
                model.getVersion(),
                model.getSignatureSha256().toByteArray(),
                outputType,
                limits,
                plan.queryText(),
                textSources.get(0).scoringNode().column()));
    }

    private org.pangrova.protocol.v2.ModelAvailability resolveRerankModel(
            String modelName) {
        var required = BuiltInCapabilityCatalog.crossEncoderRerankPlanRef();
        List<org.pangrova.protocol.v2.ModelAvailability> matches =
                workerManager.getActiveWorkers().stream()
                        .flatMap(worker -> worker.capabilityInventory()
                                .getModelsList().stream())
                        .filter(availability -> availability.getModel().getId()
                                .equals(modelName))
                        .filter(availability -> availability
                                .getCapabilityContractsList().stream()
                                .map(org.pangrova.protocol.v2.ModelCapabilityContract
                                        ::getCapability)
                                .anyMatch(advertised -> WorkerCapabilityMatcher
                                        .matchesCapabilityRef(advertised, required)))
                        .distinct()
                        .toList();
        if (matches.isEmpty()) {
            throw new PangrovaException(
                    PangrovaErrorCode.PLANNER_VALIDATION_FAILURE,
                    "No active Worker advertises exact Cross-Encoder model '"
                            + modelName + "'");
        }
        if (matches.size() != 1) {
            throw new PangrovaException(
                    PangrovaErrorCode.PLANNER_VALIDATION_FAILURE,
                    "Cross-Encoder model '" + modelName
                            + "' resolves to multiple active versions/signatures; "
                            + "publish one exact model identity before dispatch");
        }
        return matches.get(0);
    }

    private static RerankerPlanNode.RerankerStrategy fusionStrategy(
            RerankerPlanNode plan) {
        if (plan.strategy() instanceof RerankerPlanNode.RrfStrategy
                || plan.strategy()
                        instanceof RerankerPlanNode.LinearCombinationStrategy) {
            return plan.strategy();
        }
        if (plan.strategy() instanceof RerankerPlanNode.CrossEncoderStrategy) {
            return new RerankerPlanNode.RrfStrategy(DEFAULT_RRF_RANK_CONSTANT);
        }
        if (plan.strategy() instanceof RerankerPlanNode.CascadeStrategy cascade
                && cascade.stages().size() == 2
                && (cascade.stages().get(0)
                        instanceof RerankerPlanNode.RrfStrategy
                    || cascade.stages().get(0)
                        instanceof RerankerPlanNode.LinearCombinationStrategy)
                && cascade.stages().get(1)
                        instanceof RerankerPlanNode.CrossEncoderStrategy) {
            return cascade.stages().get(0);
        }
        throw new PangrovaException(
                PangrovaErrorCode.PLANNER_VALIDATION_FAILURE,
                "Cascade must be exactly [RRF|LINEAR, CROSS_ENCODER]");
    }

    private static java.util.Optional<RerankerPlanNode.CrossEncoderStrategy>
            crossEncoderStrategy(RerankerPlanNode plan) {
        if (plan.strategy() instanceof RerankerPlanNode.CrossEncoderStrategy cross) {
            return java.util.Optional.of(cross);
        }
        if (plan.strategy() instanceof RerankerPlanNode.CascadeStrategy cascade) {
            fusionStrategy(plan);
            return java.util.Optional.of(
                    (RerankerPlanNode.CrossEncoderStrategy) cascade.stages().get(1));
        }
        return java.util.Optional.empty();
    }

    private static int preRerankLimit(RerankerPlanNode plan) {
        return crossEncoderStrategy(plan)
                .map(RerankerPlanNode.CrossEncoderStrategy::topN)
                .orElse(plan.topK());
    }

    private static int hybridCandidateLimit(
            DispatchQueryContext ctx,
            RerankerPlanNode plan,
            RerankerPlanNode.RetrievalSource source) {
        int sourceCount = plan.sources().size();
        int perSourceLimit = RetrievalMergeBinding.MAX_CANDIDATE_ROWS / sourceCount;
        int mergeLimit = preRerankLimit(plan);
        if (mergeLimit > perSourceLimit) {
            throw new PangrovaException(
                    PangrovaErrorCode.PLANNER_VALIDATION_FAILURE,
                    "Hybrid Top-K exceeds the bounded Native fusion candidate budget");
        }
        long expanded = Math.min(
                Math.multiplyExact((long) mergeLimit, 4L),
                Math.addExact((long) mergeLimit, 1_000L));
        long desired = Math.min(expanded, perSourceLimit);
        if (source instanceof RerankerPlanNode.VectorSource vector) {
            desired = Math.max(desired, ctx.vectorSearchNode()
                    .filter(node -> node.vectorColumn().equals(vector.column()))
                    .map(node -> (long) node.hybridStrategy().effectiveK())
                    .orElse((long) mergeLimit));
        }
        return Math.toIntExact(Math.min(desired, perSourceLimit));
    }

    private static int vectorCandidateLimit(
            DispatchQueryContext ctx,
            org.pangrova.planner.vector.VectorSearchNode node) {
        if (ctx.rerankerPlan().isEmpty()
                || !ctx.rerankerPlan().orElseThrow().isHybrid()) {
            return node.hybridStrategy().effectiveK();
        }
        RerankerPlanNode plan = ctx.rerankerPlan().orElseThrow();
        return plan.sources().stream()
                .filter(RerankerPlanNode.VectorSource.class::isInstance)
                .map(RerankerPlanNode.VectorSource.class::cast)
                .filter(source -> source.column().equals(node.vectorColumn()))
                .findFirst()
                .map(source -> hybridCandidateLimit(ctx, plan, source))
                .orElseThrow(() -> new PangrovaException(
                        PangrovaErrorCode.PLANNER_VALIDATION_FAILURE,
                        "Vector retrieval source is absent from the hybrid merge contract"));
    }

    private static int fullTextCandidateLimit(
            DispatchQueryContext ctx, String column) {
        if (ctx.rerankerPlan().isEmpty()) {
            throw new PangrovaException(
                    PangrovaErrorCode.PLANNER_VALIDATION_FAILURE,
                    "Full-text retrieval artifact has no reranker source contract");
        }
        RerankerPlanNode plan = ctx.rerankerPlan().orElseThrow();
        return plan.sources().stream()
                .filter(RerankerPlanNode.FullTextSource.class::isInstance)
                .map(RerankerPlanNode.FullTextSource.class::cast)
                .filter(source -> source.scoringNode().column().equals(column))
                .findFirst()
                .map(source -> plan.isHybrid()
                        ? hybridCandidateLimit(ctx, plan, source)
                        : preRerankLimit(plan))
                .orElseThrow(() -> new PangrovaException(
                        PangrovaErrorCode.PLANNER_VALIDATION_FAILURE,
                        "Full-text retrieval source is absent from the reranker contract"));
    }

    private static List<NamedValue> bm25InvocationOptions(String queryText) {
        TypedValue query = TypedValue.newBuilder()
                .setType(LogicalType.newBuilder().setCanonicalSql("VARCHAR"))
                .setStringValue(queryText)
                .build();
        TypedValue analyzer = TypedValue.newBuilder()
                .setType(LogicalType.newBuilder().setCanonicalSql("VARCHAR"))
                .setStringValue("standard-v1")
                .build();
        TypedValue k1 = TypedValue.newBuilder()
                .setType(LogicalType.newBuilder().setCanonicalSql("DOUBLE"))
                .setDoubleValue(DEFAULT_BM25_K1)
                .build();
        TypedValue b = TypedValue.newBuilder()
                .setType(LogicalType.newBuilder().setCanonicalSql("DOUBLE"))
                .setDoubleValue(DEFAULT_BM25_B)
                .build();
        return List.of(
                NamedValue.newBuilder().setName("query_text").setValue(query).build(),
                NamedValue.newBuilder().setName("analyzer").setValue(analyzer).build(),
                NamedValue.newBuilder().setName("k1").setValue(k1).build(),
                NamedValue.newBuilder().setName("b").setValue(b).build());
    }

    private static List<NamedValue> vectorInvocationOptions(
            float[] queryVector, CapabilityRef retrievalCapability, int searchBudget,
            org.pangrova.spi.index.vector.DistanceMetric distanceMetric) {
        ByteBuffer packed = ByteBuffer.allocate(Math.multiplyExact(
                queryVector.length, Float.BYTES)).order(ByteOrder.LITTLE_ENDIAN);
        for (float value : queryVector) {
            if (!Float.isFinite(value)) {
                throw new IllegalArgumentException(
                        "ANN query vector must contain only finite values");
            }
            packed.putFloat(value);
        }
        TypedValue vector = TypedValue.newBuilder()
                .setType(LogicalType.newBuilder().setCanonicalSql(
                        "VECTOR(" + queryVector.length + ", FLOAT)"))
                .setTensorValue(TensorValue.newBuilder()
                        .setElementType(TensorValue.ElementType.ELEMENT_TYPE_FLOAT32)
                        .addShape(queryVector.length)
                        .setPackedLittleEndianValues(ByteString.copyFrom(packed.array())))
                .build();
        TypedValue metric = TypedValue.newBuilder()
                .setType(LogicalType.newBuilder().setCanonicalSql("VARCHAR"))
                .setStringValue(distanceMetric.name())
                .build();
        TypedValue searchList = TypedValue.newBuilder()
                .setType(LogicalType.newBuilder().setCanonicalSql("BIGINT"))
                .setInt64Value(searchBudget)
                .build();
        String capabilityId = retrievalCapability.getCapabilityId();
        final String parameterName;
        if (BuiltInCapabilityCatalog.HNSW_RETRIEVAL_ID.equals(capabilityId)) {
            parameterName = "ef_search";
        } else if (BuiltInCapabilityCatalog.IVF_PQ_RETRIEVAL_ID.equals(capabilityId)) {
            parameterName = "nprobes";
        } else if (BuiltInCapabilityCatalog.DISKANN_RETRIEVAL_ID.equals(capabilityId)
                || BuiltInCapabilityCatalog.FILTERED_DISKANN_RETRIEVAL_ID.equals(capabilityId)) {
            parameterName = "search_list_size";
        } else {
            throw new IllegalArgumentException(
                    "Unsupported Native vector retrieval capability: " + capabilityId);
        }
        return List.of(
                NamedValue.newBuilder().setName("query_vector").setValue(vector).build(),
                NamedValue.newBuilder().setName("metric").setValue(metric).build(),
                NamedValue.newBuilder().setName(parameterName)
                        .setValue(searchList).build());
    }

}
