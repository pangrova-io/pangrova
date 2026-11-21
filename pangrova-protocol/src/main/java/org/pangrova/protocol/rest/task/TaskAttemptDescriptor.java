package org.pangrova.protocol.rest.task;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Fenced identity for one execution attempt of a logical stage partition.
 *
 * <p>The token is monotonically ordered within a logical partition. It is not
 * a credential; transport authorization is provided by producer-issued signed
 * tickets. Workers use this identity only to reject stale/replayed bindings.</p>
 */
public record TaskAttemptDescriptor(
        @JsonProperty("plan_epoch") long planEpoch,
        @JsonProperty("attempt_id") int attemptId,
        @JsonProperty("fencing_token") long fencingToken) {

    private static final long ATTEMPTS_PER_EPOCH = 1L << 32;

    public TaskAttemptDescriptor {
        if (planEpoch <= 0) {
            throw new IllegalArgumentException("planEpoch must be positive");
        }
        if (attemptId < 0) {
            throw new IllegalArgumentException("attemptId must be non-negative");
        }
        if (fencingToken <= 0) {
            throw new IllegalArgumentException("fencingToken must be positive");
        }
    }

    /** Creates a deterministic monotonically ordered token for one plan epoch. */
    public static TaskAttemptDescriptor of(long planEpoch, int attemptId) {
        long base = Math.multiplyExact(planEpoch, ATTEMPTS_PER_EPOCH);
        long token = Math.addExact(base, Integer.toUnsignedLong(attemptId) + 1L);
        return new TaskAttemptDescriptor(planEpoch, attemptId, token);
    }

    /** Creates the REST status projection of a canonical attempt identity. */
    public static TaskAttemptDescriptor from(
            org.pangrova.protocol.v2.TaskAttemptId attempt) {
        if (attempt == null) {
            throw new IllegalArgumentException("attempt must not be null");
        }
        return new TaskAttemptDescriptor(
                attempt.getPlanEpoch(), attempt.getAttemptId(),
                attempt.getFencingToken());
    }

    /** Returns the next retry/hedge identity in the same plan epoch. */
    public TaskAttemptDescriptor nextAttempt() {
        return of(planEpoch, Math.incrementExact(attemptId));
    }
}
