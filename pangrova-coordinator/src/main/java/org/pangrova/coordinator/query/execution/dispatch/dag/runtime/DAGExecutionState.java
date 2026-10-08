package org.pangrova.coordinator.query.execution.dispatch.dag.runtime;

import org.pangrova.coordinator.query.session.CoordinatorSessionContext;
import org.pangrova.coordinator.query.execution.dispatch.DispatchResult;
import org.pangrova.coordinator.query.execution.dispatch.dag.DAGDispatchEngine.StageDispatchRecord;
import org.pangrova.coordinator.query.execution.dispatch.dag.WorkerSelector;
import org.pangrova.coordinator.query.execution.dispatch.dag.control.DurableStageRuntime;
import org.pangrova.coordinator.query.execution.dispatch.dag.control.StageExecutionContext;
import org.pangrova.coordinator.query.stage.StageDAG;
import org.pangrova.planner.SqlCompiler;
import org.pangrova.spi.annotation.Internal;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

/** Per-execution state for the sole DAG engine; durable execution has no live planner or user session. */
@Internal
public record DAGExecutionState(StageExecutionContext execution, StageDAG dag, SqlCompiler compiler,
        WorkerSelector workerSelector, DurableStageRuntime durable, List<StageDispatchRecord> dispatched,
        ConcurrentHashMap<Integer, DispatchResult> results, CompletableFuture<DispatchResult> completion,
        AtomicBoolean cancelled, AtomicInteger runningStageCount,
        ConcurrentHashMap<Integer, Throwable> dispatchFailures, Consumer<DispatchResult> resultReadySink,
        ConcurrentHashMap<Integer, org.pangrova.planner.SerializedPlanFragment> preparedPlans) {
    /** Allocates only volatile bookkeeping, never a second planning or execution path. */
    public static DAGExecutionState create(StageExecutionContext execution, StageDAG dag, SqlCompiler compiler,
            WorkerSelector selector, DurableStageRuntime durable, Consumer<DispatchResult> sink) {
        return new DAGExecutionState(execution, dag, compiler, selector, durable, new CopyOnWriteArrayList<>(),
                new ConcurrentHashMap<>(), new CompletableFuture<>(), new AtomicBoolean(), new AtomicInteger(),
                new ConcurrentHashMap<>(), sink, new ConcurrentHashMap<>());
    }
    public String queryId() { return execution.executionId(); }
    public CoordinatorSessionContext session() { return execution.requireLiveSession(); }

    /** Serializes the actual live stage, preserving the pre-optimization output ordinal names. */
    public org.pangrova.planner.SerializedPlanFragment serializePlan(
            org.pangrova.coordinator.query.stage.StageNode plan,
            org.pangrova.coordinator.query.execution.dispatch.dag.DAGQueryContext context) {
        List<String> columns = plan.stagePlan().fieldNames();
        if (dag.findDownstreamEdge(plan.stageId()) == null && context != null
                && !context.preOptColumnNames().isEmpty()) {
            columns = context.preOptColumnNames();
        }
        var serialized = plan.stagePlan().toSerializedPlan(compiler, columns);
        if (plan.stagePlan().mode() == org.pangrova.planner.StagePlanAccess.Mode.DURABLE_REPLAY) {
            return serialized;
        }
        var incoming = dag.stageEdges().stream()
                .filter(edge -> edge.downstreamStageId() == plan.stageId())
                .map(edge -> new org.pangrova.planner.StageTransfer(edge.upstreamStageId(), edge.downstreamStageId(),
                        edge.shuffleType(), edge.distributionKeys(), edge.exchangeId(), edge.partitionCount(),
                        edge.batchSerde(), edge.requireComplete()))
                .toList();
        return org.pangrova.planner.AuthorizedStageGraph.bindExchangeInputs(
                plan.stageId(), serialized, incoming, preparedPlans);
    }

    /** Publishes the final bound producer contract before any task can make its stage RUNNING. */
    public void recordPreparedPlan(int stageId, org.pangrova.protocol.BoundExecutionPlan bound) {
        preparedPlans.put(stageId, new org.pangrova.planner.SerializedPlanFragment(
                bound.planBytes(), List.of(), List.of()));
    }
}
