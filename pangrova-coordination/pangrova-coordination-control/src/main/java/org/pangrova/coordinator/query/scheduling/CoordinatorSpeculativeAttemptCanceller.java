package org.pangrova.coordinator.query.scheduling;

import org.pangrova.coordinator.task.TaskCancelRetrier;
import org.pangrova.scheduler.api.speculation.SpeculativeAttemptCanceller;

/** Adapts the coordinator's scheduled cancel retry owner to the scheduler port. */
public final class CoordinatorSpeculativeAttemptCanceller implements SpeculativeAttemptCanceller {

    private final TaskCancelRetrier retrier;

    public CoordinatorSpeculativeAttemptCanceller(TaskCancelRetrier retrier) {
        this.retrier = retrier;
    }

    @Override
    public void cancelWithRetry(String queryId, String taskId, String workerHost, int httpPort) {
        retrier.cancelWithRetry(queryId, taskId, workerHost, httpPort);
    }
}
