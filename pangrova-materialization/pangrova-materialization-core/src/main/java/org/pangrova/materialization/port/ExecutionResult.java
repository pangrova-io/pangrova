package org.pangrova.materialization.port;

import java.util.OptionalLong;
import org.pangrova.connector.spi.materialization.CommitEvidence;

/**
 * Outcome of one data-plane execution.
 *
 * @param evidence exact connector commit evidence
 * @param writtenRows rows written when the adapter can report them, for observability only
 * @param executionId adapter-scoped execution identifier
 */
public record ExecutionResult(
        CommitEvidence evidence, OptionalLong writtenRows, String executionId) {

    /** Creates a result whose adapter reported an exact row count. */
    public ExecutionResult(CommitEvidence evidence, long writtenRows, String executionId) {
        this(evidence, OptionalLong.of(writtenRows), executionId);
    }

    public ExecutionResult {
        if (evidence == null) {
            throw new IllegalArgumentException("evidence must not be null");
        }
        writtenRows = writtenRows == null ? OptionalLong.empty() : writtenRows;
        if (writtenRows.isPresent() && writtenRows.getAsLong() < 0) {
            throw new IllegalArgumentException("writtenRows must not be negative");
        }
        if (executionId == null || executionId.isBlank()) {
            throw new IllegalArgumentException("executionId must not be blank");
        }
    }
}
