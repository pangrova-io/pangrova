package org.pangrova.scheduler.speculation;

import org.pangrova.protocol.rest.task.SubmitTaskRequest;
import org.pangrova.scheduler.api.placement.PlacementWorker;

/**
 * Transport port used to issue a speculative task attempt.
 *
 * <p>Implementations own Worker RPC. Dispatch returns the acknowledged task
 * identity so the caller can verify that the Worker did not change the
 * admitted attempt.</p>
 */
public interface SpeculativeAttemptDispatcher {

    SubmitTaskRequest bindWorkerIdentity(SubmitTaskRequest request, PlacementWorker worker);

    default SubmitTaskRequest bindControlIdentity(SubmitTaskRequest request) {
        return request;
    }

    default SubmitTaskRequest bindSourceInput(SubmitTaskRequest request) {
        return request;
    }

    String dispatch(SubmitTaskRequest request, PlacementWorker worker);
}
