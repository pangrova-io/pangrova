package org.pangrova.materialization.port;

import org.pangrova.spi.materialization.CommitEvidence;

/**
 * Outcome of one data-plane execution.
 *
 * @param evidence exact connector commit evidence
 * @param writtenRows number of rows written, for observability only
 * @param executionId adapter-scoped execution identifier
 */
public record ExecutionResult(CommitEvidence evidence, long writtenRows, String executionId) {

    public ExecutionResult {
        if (evidence == null) {
            throw new IllegalArgumentException("evidence must not be null");
        }
        if (writtenRows < 0) {
            throw new IllegalArgumentException("writtenRows must not be negative");
        }
        if (executionId == null || executionId.isBlank()) {
            throw new IllegalArgumentException("executionId must not be blank");
        }
    }
}
