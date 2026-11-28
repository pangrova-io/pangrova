package org.pangrova.coordinator.streaming;

import org.pangrova.coordinator.scheduler.TaskDispatcher;
import org.pangrova.coordinator.scheduler.placement.WorkerSelector;
import org.pangrova.coordinator.task.CoordinatorTaskTracker;
import org.pangrova.protocol.rest.task.SubmitTaskRequest;
import org.pangrova.query.api.journal.TaskStateEvent;
import org.pangrova.protocol.v2.TaskRequest;

import java.util.List;
import java.util.Objects;
import java.util.function.Consumer;

/** Real Coordinator transport adapter for one planned CDC task. */
public final class WorkerCdcTaskDispatcher implements CdcTaskDispatchPort {

    private final TaskDispatcher dispatcher;
    private final WorkerSelector selector;
    private final CoordinatorTaskTracker tracker;
    private final Consumer<List<TaskStateEvent>> recordIssuances;

    /** Uses the shared execution journal to fence CDC issuance before the Worker can start. */
    public WorkerCdcTaskDispatcher(
            TaskDispatcher dispatcher,
            WorkerSelector selector,
            CoordinatorTaskTracker tracker,
            Consumer<List<TaskStateEvent>> recordIssuances) {
        this.dispatcher = Objects.requireNonNull(dispatcher, "dispatcher");
        this.selector = Objects.requireNonNull(selector, "selector");
        this.tracker = Objects.requireNonNull(tracker, "tracker");
        this.recordIssuances = Objects.requireNonNull(recordIssuances, "recordIssuances");
    }

    @Override
    public void dispatch(TaskRequest request) {
        Objects.requireNonNull(request, "request");
        String queryId = request.getAttempt().getQueryOrJobId();
        String taskId = request.getAttempt().getTaskId();
        var worker = selector.selectForCdc(request.getSecurity().getTenantId());
        var submitted = dispatcher.bindControlIdentity(
                dispatcher.bindWorkerIdentity(SubmitTaskRequest.from(request), worker));
        org.pangrova.protocol.task.CanonicalTaskRequests.validateForAdmission(submitted.taskRequest());
        var issuedOwner = submitted.taskRequest().getAttempt().getExecutionOwner();
        if (!issuedOwner.equals(request.getAttempt().getExecutionOwner())) {
            throw new IllegalStateException("CDC request changed its execution owner while binding the Worker");
        }
        tracker.prepareTaskDispatch(submitted, worker);
        try {
            recordIssuances.accept(List.of(tracker.issuanceFact(submitted, worker)));
            tracker.enterTaskDispatch(submitted);
        } catch (RuntimeException | Error failure) {
            try {
                tracker.unregisterUndispatchedTask(queryId, taskId);
            } catch (RuntimeException cleanup) {
                failure.addSuppressed(cleanup);
            }
            throw failure;
        }
        var acknowledged = dispatcher.dispatchBound(submitted, worker);
        if (acknowledged == null || !taskId.equals(acknowledged.taskId())) {
            throw new IllegalStateException("Worker acknowledgement changed the issued CDC task identity");
        }
    }
}
