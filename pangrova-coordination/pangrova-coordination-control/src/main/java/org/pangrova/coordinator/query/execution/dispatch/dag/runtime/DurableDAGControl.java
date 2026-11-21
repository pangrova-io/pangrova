package org.pangrova.coordinator.query.execution.dispatch.dag.runtime;

import org.pangrova.execution.task.DispatchResult;
import org.pangrova.coordinator.query.execution.dispatch.dag.control.DurableStageRuntime;
import org.pangrova.shuffle.api.coordinator.RecoverableShuffleControl;
import org.pangrova.shuffle.api.ShuffleExecutionOwner;
import org.pangrova.common.api.annotation.Internal;

import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;

/** Owns volatile durable control passes in the shared DAG, retaining external allocations on failed passes. */
@Internal
public final class DurableDAGControl {
    private final DAGLifecycleController lifecycle;
    private final RecoverableShuffleControl shuffle;
    private final ConcurrentHashMap<String, DAGExecutionState> executions = new ConcurrentHashMap<>();

    /** Requires the same lifecycle and real external shuffle manager as ordinary stage dispatch. */
    public DurableDAGControl(DAGLifecycleController lifecycle, RecoverableShuffleControl shuffle) {
        this.lifecycle = java.util.Objects.requireNonNull(lifecycle, "lifecycle");
        this.shuffle = shuffle;
    }

    /** Registers control, then invokes the sole engine's ordinary listener and root-stage scheduling. */
    public CompletableFuture<DispatchResult> start(DurableStageRuntime runtime, Consumer<DAGExecutionState> schedule) {
        if (shuffle == null) {
            throw new IllegalStateException("Durable DAG requires the actual governed external shuffle lifecycle");
        }
        var state = DAGExecutionState.create(runtime.control(), runtime.dag(), null, null, runtime, ignored -> { });
        if (executions.putIfAbsent(state.queryId(), state) != null) {
            throw new IllegalStateException("Exact durable execution is already active in the shared DAG engine");
        }
        state.completion().whenComplete((result, failure) -> settle(state, runtime, failure));
        try {
            shuffle.bindDurableEvidence(state.queryId(), runtime.shuffleEvidence());
            lifecycle.registerQuery(state.queryId(), state.dag(), runtime.queryContext());
            schedule.accept(state);
        } catch (RuntimeException failure) {
            lifecycle.failDispatch(state.queryId(), state.dag(), state.cancelled(), state.dispatched());
            state.completion().completeExceptionally(failure);
        }
        return state.completion();
    }

    private void settle(DAGExecutionState state, DurableStageRuntime runtime, Throwable failure) {
        try {
            if (failure != null) {
                var owner = runtime.control().authority().executionAnchor();
                shuffle.detachDurableControl(state.queryId(), new ShuffleExecutionOwner(
                        owner.ownerId(), owner.ownerIncarnation(), owner.fencingToken()));
            }
        } finally {
            executions.remove(state.queryId(), state);
        }
    }

    /** Cancels actual tasks and this pass, without deleting replay evidence or external producer data. */
    public void cancel(String executionId) {
        var state = executions.get(executionId);
        if (state != null) {
            lifecycle.failDispatch(executionId, state.dag(), state.cancelled(), state.dispatched());
            state.completion().completeExceptionally(new CancellationException(
                    "Durable execution control stopped; persisted allocations remain available for recovery"));
        }
    }
}
