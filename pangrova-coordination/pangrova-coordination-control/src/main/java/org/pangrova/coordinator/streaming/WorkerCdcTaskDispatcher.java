package org.pangrova.coordinator.streaming;

import org.pangrova.coordinator.scheduler.TaskDispatcher;
import org.pangrova.coordinator.scheduler.placement.WorkerSelector;
import org.pangrova.coordinator.task.CoordinatorTaskTracker;
import org.pangrova.protocol.rest.task.SubmitTaskRequest;
import org.pangrova.protocol.rest.task.TaskAttemptDescriptor;
import org.pangrova.protocol.v2.TaskRequest;

import java.util.Objects;

/** Real Coordinator transport adapter for one planned CDC task. */
public final class WorkerCdcTaskDispatcher implements CdcTaskDispatchPort {

    private final TaskDispatcher dispatcher;
    private final WorkerSelector selector;
    private final CoordinatorTaskTracker tracker;

    public WorkerCdcTaskDispatcher(
            TaskDispatcher dispatcher,
            WorkerSelector selector,
            CoordinatorTaskTracker tracker) {
        this.dispatcher = Objects.requireNonNull(dispatcher, "dispatcher");
        this.selector = Objects.requireNonNull(selector, "selector");
        this.tracker = Objects.requireNonNull(tracker, "tracker");
    }

    @Override
    public void dispatch(TaskRequest request) {
        Objects.requireNonNull(request, "request");
        String queryId = request.getAttempt().getQueryOrJobId();
        String taskId = request.getAttempt().getTaskId();
        var worker = selector.select(request.getSecurity().getTenantId());
        var submitted = SubmitTaskRequest.from(request);
        tracker.registerTask(queryId, taskId);
        boolean dispatched = false;
        try {
            tracker.registerTaskRequest(taskId, submitted);
            tracker.registerTaskAttempt(taskId, TaskAttemptDescriptor.from(request.getAttempt()));
            tracker.markScheduled(queryId, taskId);
            dispatcher.dispatch(submitted, worker);
            dispatched = true;
        } finally {
            if (!dispatched) {
                try {
                    tracker.unregisterUndispatchedTask(queryId, taskId);
                } catch (RuntimeException cleanup) {
                    // Preserve the original transport/registration failure.
                }
            }
        }
    }
}
