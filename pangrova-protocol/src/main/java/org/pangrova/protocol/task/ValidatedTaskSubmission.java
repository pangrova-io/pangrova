package org.pangrova.protocol.task;

import java.util.Objects;

import org.pangrova.protocol.rest.task.SubmitTaskRequest;

/** Structural validation of one immutable network task submission. */
public record ValidatedTaskSubmission(SubmitTaskRequest submission) {

    public ValidatedTaskSubmission {
        Objects.requireNonNull(submission, "submission");
        CanonicalTaskRequests.validateForAdmission(submission.taskRequest());
    }
}
