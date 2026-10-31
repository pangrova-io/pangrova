package org.pangrova.coordinator.query.execution.dispatch.dag.binding;

import org.pangrova.coordinator.generation.GeneratedValueReadResolver;
import org.pangrova.coordinator.generation.PublishedGenerationBindingResolver.ResolvedRetrievalGeneration;
import org.pangrova.coordinator.query.session.CoordinatorSessionContext;
import org.pangrova.coordinator.query.execution.dispatch.dag.DAGQueryContext;
import org.pangrova.coordinator.query.execution.dispatch.dag.WorkerSelector;
import org.pangrova.coordinator.query.execution.dispatch.dag.retrieval.RetrievalGenerationResolver;
import org.pangrova.coordinator.query.execution.dispatch.dag.retrieval.RetrievalPlanBinder;
import org.pangrova.coordinator.query.execution.dispatch.dag.retrieval.RetrievalPlanBinder.DistributedRetrieval;
import org.pangrova.coordinator.query.execution.split.SplitFetcher;
import org.pangrova.coordinator.query.stage.StageNode;
import org.pangrova.planner.SerializedPlanFragment;
import org.pangrova.protocol.execution.BoundExecutionPlan;
import org.pangrova.protocol.type.VectorLogicalType;
import org.pangrova.protocol.v2.LogicalType;
import org.pangrova.protocol.v2.ResultColumnLogicalType;
import org.pangrova.common.api.annotation.Internal;

import java.util.List;
import java.util.Map;

/** Live binding phase of the one stage engine; durable replay consumes immutable bound templates. */
@Internal
public final class DAGStagePlanBinder {
    private final RetrievalGenerationResolver retrievalArtifactResolver;
    private final GeneratedValueReadResolver generatedValueReadResolver;

    /** Shares the query's canonical published artifact and generated-value resolvers. */
    public DAGStagePlanBinder(RetrievalGenerationResolver retrievalArtifactResolver,
            GeneratedValueReadResolver generatedValueReadResolver) {
        this.retrievalArtifactResolver = java.util.Objects.requireNonNull(retrievalArtifactResolver);
        this.generatedValueReadResolver = generatedValueReadResolver;
    }

    /** Authenticated live compilation metadata for one exact stage. */
    public record Input(SerializedPlanFragment serialized, StageNode plan,
            List<SplitFetcher.SplitWithPaths> splits, DAGQueryContext queryContext,
            CoordinatorSessionContext session, WorkerSelector workerSelector, boolean outputStage) {
        public Input {
            splits = List.copyOf(splits);
        }
    }

    /** Resolves live protected-stage bindings through the canonical model/artifact ports. */
    public BoundExecutionPlan bind(Input input) {
        var serialized = input.serialized();
        var plan = input.plan();
        var splits = input.splits();
        var queryContext = input.queryContext();
        BoundExecutionPlan generated = generatedValueReadResolver == null
                ? new BoundExecutionPlan(serialized.planBytes(), List.of())
                : generatedValueReadResolver.bindReadOverlays(serialized.planBytes(), input.session(),
                        splits.stream().map(SplitFetcher.SplitWithPaths::serializedData).toList(),
                        queryContext.snapshotOverrides());
        SerializedPlanFragment readable = new SerializedPlanFragment(generated.planBytes(),
                serialized.retrievalTopKHints(), serialized.aiInvocationHints());
        Map<String, ResolvedRetrievalGeneration> artifacts =
                retrievalArtifactResolver.resolve(readable, queryContext, splits);
        boolean outputStage = input.outputStage();
        BoundExecutionPlan retrievalPlan = bindRetrieval(
                readable, input.session().queryId(), plan.stageId(), outputStage,
                splits.isEmpty(), queryContext, artifacts);
        List<org.pangrova.protocol.v2.OperatorBinding> operators =
                new java.util.ArrayList<>(generated.operatorBindings());
        operators.addAll(retrievalPlan.operatorBindings());
        Map<String, LogicalType> modelOutputTypes = new java.util.HashMap<>();
        serialized.aiInvocationHints().forEach(hint -> {
            var binding = AiInvocationOperatorBinder.bindQuery(
                    hint, input.workerSelector(), input.session().userIdentity());
            operators.add(binding);
            if (hint.operation() == org.pangrova.planner.AiInvocationPlanHint.Operation.EMBEDDING) {
                modelOutputTypes.put(hint.modelName(), binding.getModel().getOutputType());
            }
        });
        List<ResultColumnLogicalType> resultTypes = outputStage
                ? bindResultColumnTypes(plan.stagePlan(), modelOutputTypes)
                : List.of();
        return new BoundExecutionPlan(
                retrievalPlan.planBytes(), operators, resultTypes);
    }

    /** Retains declared storage vectors and binds only exact-model vector output intents. */
    public static List<ResultColumnLogicalType> bindResultColumnTypes(
            org.pangrova.planner.StagePlanAccess fragment,
            Map<String, LogicalType> modelOutputTypes) {
        Map<Integer, ResultColumnLogicalType> columns = new java.util.TreeMap<>();
        for (var column : fragment.resultColumnTypes()) {
            VectorLogicalType.requireStorageDimension(column.getLogicalType());
            addResultColumn(columns, column, fragment.fieldNames().size());
        }
        for (var column : fragment.resultColumnModels()) {
            addResultColumn(columns, resultColumnType(column, modelOutputTypes), fragment.fieldNames().size());
        }
        return List.copyOf(columns.values());
    }

    private static void addResultColumn(Map<Integer, ResultColumnLogicalType> columns,
            ResultColumnLogicalType column, int width) {
        if (column.getColumnIndex() < 0 || column.getColumnIndex() >= width
                || columns.putIfAbsent(column.getColumnIndex(), column) != null) {
            throw new IllegalArgumentException("Result logical types have an invalid or duplicate output ordinal");
        }
    }

    private static ResultColumnLogicalType resultColumnType(
            org.pangrova.planner.PlanFragment.ResultColumnModel column,
            Map<String, LogicalType> modelOutputTypes) {
        LogicalType type = modelOutputTypes.get(column.modelName());
        if (type == null) {
            throw new IllegalStateException(
                    "VECTOR result column has no bound model: " + column.modelName());
        }
        VectorLogicalType.requireDimension(type);
        return ResultColumnLogicalType.newBuilder()
                .setColumnIndex(column.columnIndex())
                .setLogicalType(type)
                .build();
    }

    private static BoundExecutionPlan bindRetrieval(
            SerializedPlanFragment serialized,
            String queryId,
            int stageId,
            boolean outputStage,
            boolean hasNoSplits,
            DAGQueryContext context,
            Map<String, ResolvedRetrievalGeneration> artifacts) {
        boolean hasPipeline = context != null
                && (context.retrievalMerge().isPresent()
                        || context.rerank().isPresent());
        if (hasPipeline && RetrievalPlanBinder.shouldBindDistributed(
                serialized, outputStage, hasNoSplits)) {
            return RetrievalPlanBinder.bindDistributed(new DistributedRetrieval(
                    serialized, queryId, stageId, outputStage, hasNoSplits,
                    context.retrievalMerge(), context.rerank(), artifacts));
        }
        return RetrievalPlanBinder.bindTypedCapabilities(
                serialized, queryId, stageId, artifacts);
    }

}
