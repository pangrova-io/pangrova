package org.pangrova.server.coordinator.exception;

import java.util.Objects;

/** Submission failed after registration, or registration could not be ruled out. */
public final class RecordedSubmissionException extends RuntimeException {
    private static final long serialVersionUID = 1L;

    private final String queryId;

    public RecordedSubmissionException(String queryId, RuntimeException cause) {
        super(cause);
        this.queryId = Objects.requireNonNull(queryId, "queryId");
    }

    public String queryId() {
        return queryId;
    }

    public RuntimeException failure() {
        return (RuntimeException) getCause();
    }
}
