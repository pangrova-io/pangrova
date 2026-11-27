package org.pangrova.coordinator.query.execution.dispatch.dag.runtime;

import com.google.protobuf.ByteString;
import org.pangrova.query.api.session.QuerySession;
import org.pangrova.execution.task.DispatchResult;
import org.pangrova.execution.task.StageDispatchRecord;
import org.pangrova.coordinator.scheduler.placement.WorkerSelector;
import org.pangrova.coordinator.query.execution.dispatch.dag.control.DurableStageRuntime;
import org.pangrova.execution.authority.StageExecutionContext;
import org.pangrova.coordinator.query.stage.StageDAG;
import org.pangrova.planner.analysis.CompiledStagePlan;
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
        AtomicBoolean cancelled, AtomicInteger runningStageCount, AtomicInteger downstreamWakeups,
        ConcurrentHashMap<Integer, Throwable> dispatchFailures, Consumer<DispatchResult> resultReadySink,
        List<BuildFilterGate> buildFilters) {
    /** Allocates only volatile bookkeeping, never a second planning or execution path. */
    public static DAGExecutionState create(StageExecutionContext execution, StageDAG dag, SqlCompiler compiler,
            WorkerSelector selector, DurableStageRuntime durable, Consumer<DispatchResult> sink) {
        return create(execution, dag, compiler, selector, durable, sink, List.of());
    }

    /** Installs the independently selected gates for an eligible live read query. */
    public static DAGExecutionState create(StageExecutionContext execution, StageDAG dag, SqlCompiler compiler,
            WorkerSelector selector, DurableStageRuntime durable, Consumer<DispatchResult> sink,
            List<BuildFilterGate> buildFilters) {
        return new DAGExecutionState(execution, dag, compiler, selector, durable, new CopyOnWriteArrayList<>(),
                new ConcurrentHashMap<>(), new CompletableFuture<>(), new AtomicBoolean(), new AtomicInteger(),
                new AtomicInteger(),
                new ConcurrentHashMap<>(), sink, List.copyOf(buildFilters));
    }
    public String queryId() { return execution.executionId(); }
    public QuerySession session() { return execution.requireLiveSession(); }

    /** Looks up the independent gate that can delay this probe. */
    public BuildFilterGate filterForProbe(int stageId) {
        for (var filter : buildFilters) {
            if (filter.isProbe(stageId)) return filter;
        }
        return null;
    }

    /** Looks up the independent gate that needs this build's winning summaries. */
    public BuildFilterGate filterForBuild(int stageId) {
        for (var filter : buildFilters) {
            if (filter.plan().buildStageId() == stageId) return filter;
        }
        return null;
    }

    /** Compiles the actual live stage with the pre-optimization output ordinal names. */
    public CompiledStagePlan compilePlan(
            org.pangrova.coordinator.query.stage.StageNode plan,
            List<String> preOptColumnNames) {
        return bindLiveExchangeInputs(plan, compileUnboundPlan(plan, preOptColumnNames));
    }

    /** Compiles the logical exchange shape before the merge binds its source tag. */
    private CompiledStagePlan compileUnboundPlan(
            org.pangrova.coordinator.query.stage.StageNode plan, List<String> preOptColumnNames) {
        List<String> columns = plan.stagePlan().fieldNames();
        if (dag.findDownstreamEdge(plan.stageId()) == null && !preOptColumnNames.isEmpty()) {
            columns = List.copyOf(preOptColumnNames);
        }
        return plan.stagePlan().requireLiveFragment().compileStagePlan(compiler, columns);
    }

    /** Validates the Consumer's input against the dispatched producer wire schemas. */
    private CompiledStagePlan bindLiveExchangeInputs(
            org.pangrova.coordinator.query.stage.StageNode plan, CompiledStagePlan compiled) {
        var incoming = incomingTransfers(plan.stageId());
        if (incoming.isEmpty()) return compiled;
        return AuthorizedStageGraph.bindLiveExchangeInputs(
                plan.stageId(), compiled, incoming, producerPlans(incoming));
    }

    private List<StageTransfer> incomingTransfers(int stageId) {
        return dag.stageEdges().stream().filter(edge -> edge.downstreamStageId() == stageId)
                .map(edge -> new StageTransfer(edge.upstreamStageId(), edge.downstreamStageId(),
                        edge.shuffleType(), edge.distributionKeys(), edge.exchangeId(), edge.partitionCount(),
                        edge.batchSerde(), edge.requireComplete())).toList();
    }

    private Map<Integer, List<ByteString>> producerPlans(List<StageTransfer> incoming) {
        return dispatched.stream()
                .filter(record -> record.request() != null)
                .filter(record -> incoming.stream().anyMatch(edge -> edge.upstreamStageId() == record.stageId()))
                .collect(Collectors.groupingBy(StageDispatchRecord::stageId,
                        Collectors.mapping(record -> record.request().taskRequest().getSubstraitPlan(),
                                Collectors.toList())));
    }
}
