package org.pangrova.coordinator.query.execution.dispatch.dag.runtime;

import org.pangrova.query.api.session.QuerySession;
import org.pangrova.execution.task.DispatchResult;
import org.pangrova.execution.task.StageDispatchRecord;
import org.pangrova.coordinator.scheduler.placement.WorkerSelector;
import org.pangrova.coordinator.query.execution.dispatch.dag.control.DurableStageRuntime;
import org.pangrova.execution.authority.StageExecutionContext;
import org.pangrova.coordinator.query.stage.StageDAG;
import org.pangrova.planner.analysis.SqlCompiler;
import org.pangrova.planner.analysis.AuthorizedStageGraph;
import org.pangrova.planner.analysis.StageTransfer;
import org.pangrova.common.api.annotation.Internal;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
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
        ConcurrentHashMap<Integer, Throwable> dispatchFailures, Consumer<DispatchResult> resultReadySink) {
    /** Allocates only volatile bookkeeping, never a second planning or execution path. */
    public static DAGExecutionState create(StageExecutionContext execution, StageDAG dag, SqlCompiler compiler,
            WorkerSelector selector, DurableStageRuntime durable, Consumer<DispatchResult> sink) {
        return new DAGExecutionState(execution, dag, compiler, selector, durable, new CopyOnWriteArrayList<>(),
                new ConcurrentHashMap<>(), new CompletableFuture<>(), new AtomicBoolean(), new AtomicInteger(),
                new ConcurrentHashMap<>(), sink);
    }
    public String queryId() { return execution.executionId(); }
    public QuerySession session() { return execution.requireLiveSession(); }

    /** Serializes the actual live stage, preserving the pre-optimization output ordinal names. */
    public org.pangrova.planner.api.fragment.SerializedPlanFragment serializePlan(
            org.pangrova.coordinator.query.stage.StageNode plan,
            List<String> preOptColumnNames) {
        List<String> columns = plan.stagePlan().fieldNames();
        if (dag.findDownstreamEdge(plan.stageId()) == null && !preOptColumnNames.isEmpty()) {
            columns = List.copyOf(preOptColumnNames);
        }
        var serialized = plan.stagePlan().toSerializedPlan(compiler, columns);
        var incoming = dag.stageEdges().stream().filter(edge -> edge.downstreamStageId() == plan.stageId())
                .map(edge -> new StageTransfer(edge.upstreamStageId(), edge.downstreamStageId(),
                        edge.shuffleType(), edge.distributionKeys(), edge.exchangeId(), edge.partitionCount(),
                        edge.batchSerde(), edge.requireComplete())).toList();
        if (incoming.isEmpty()) return serialized;
        Map<Integer, List<byte[]>> producerPlans = dispatched.stream()
                .filter(record -> record.request() != null)
                .filter(record -> incoming.stream().anyMatch(edge -> edge.upstreamStageId() == record.stageId()))
                .collect(Collectors.groupingBy(StageDispatchRecord::stageId,
                        Collectors.mapping(record -> record.request().taskRequest().getSubstraitPlan().toByteArray(),
                                Collectors.toList())));
        return AuthorizedStageGraph.bindLiveExchangeInputs(plan.stageId(), serialized, incoming, producerPlans);
    }
}
