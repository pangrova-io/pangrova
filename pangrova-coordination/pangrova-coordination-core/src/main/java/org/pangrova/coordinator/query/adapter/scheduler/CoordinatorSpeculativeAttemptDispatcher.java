package org.pangrova.coordinator.query.adapter.scheduler;

import org.pangrova.coordinator.scheduler.TaskDispatcher;

import org.pangrova.coordinator.worker.WorkerInfo;
import org.pangrova.protocol.rest.task.SubmitTaskRequest;
import org.pangrova.scheduler.api.placement.PlacementWorker;
import org.pangrova.scheduler.core.speculation.SpeculativeAttemptDispatcher;

/** Adapts the coordinator Worker RPC dispatcher to the speculation transport port. */
public final class CoordinatorSpeculativeAttemptDispatcher implements SpeculativeAttemptDispatcher {

    private final TaskDispatcher dispatcher;

    public CoordinatorSpeculativeAttemptDispatcher(TaskDispatcher dispatcher) {
        this.dispatcher = dispatcher;
    }

    @Override
    public SubmitTaskRequest bindWorkerIdentity(SubmitTaskRequest request, PlacementWorker worker) {
        return dispatcher.bindWorkerIdentity(request, requireWorkerInfo(worker));
    }

    @Override
    public SubmitTaskRequest bindControlIdentity(SubmitTaskRequest request) {
        return dispatcher.bindControlIdentity(request);
    }

    @Override
    public SubmitTaskRequest bindSourceInput(SubmitTaskRequest request) {
        return dispatcher.bindSourceInput(request);
    }

    @Override
    public String dispatch(SubmitTaskRequest request, PlacementWorker worker) {
        return dispatcher.dispatch(request, requireWorkerInfo(worker)).taskId();
    }

    private static WorkerInfo requireWorkerInfo(PlacementWorker worker) {
        if (worker instanceof WorkerInfo info) {
            return info;
        }
        throw new IllegalArgumentException(
                "Coordinator Worker RPC requires a WorkerInfo dispatch target");
    }
}
