package org.pangrova.protocol.task;

import java.util.Objects;

import org.pangrova.protocol.rest.task.SubmitTaskRequest;
import org.pangrova.protocol.v2.ExecutionCallbackContext;

/** Structural validation of one immutable network task submission. */
public record ValidatedTaskSubmission(SubmitTaskRequest submission) {

    public ValidatedTaskSubmission {
        Objects.requireNonNull(submission, "submission");
        CanonicalTaskRequests.validateForAdmission(submission.taskRequest());
    }

    /** Projects the exact admitted callback authority without revalidating the plan. */
    public ExecutionCallbackContext callbackContext() {
        return CanonicalTaskRequests.projectCallbackContext(submission.taskRequest());
    }
}
