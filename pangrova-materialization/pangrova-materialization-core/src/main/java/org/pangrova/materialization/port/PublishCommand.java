package org.pangrova.materialization.port;

import org.pangrova.materialization.domain.AttemptId;
import org.pangrova.materialization.domain.CommandId;
import org.pangrova.materialization.domain.ExecutionFence;
import org.pangrova.materialization.domain.Generation;
import org.pangrova.materialization.domain.MaterializationId;
import org.pangrova.materialization.domain.RefreshAttempt;
import org.pangrova.connector.spi.materialization.CommitEvidence;

/**
 * Atomic publication command.
 *
 * <p>One repository transaction must verify the fence and evidence, advance
 * the object head with CAS, mark the generation published, retire the previous
 * head, settle the attempt, record the command receipt, and append the outbox
 * event. A lost CAS is not retried implicitly; the caller reconciles.
 *
 * @param object owning object
 * @param expectedObjectRevision head revision observed before publication
 * @param generation candidate generation
 * @param attempt attempt that produced the generation
 * @param evidence exact commit evidence
 * @param fence live publication fence; the reservation fence on the synchronous
 *     path, a fresh fence under the reconciler's authority on recovery
 * @param commandId command idempotency key
 */
public record PublishCommand(
        MaterializationId object,
        long expectedObjectRevision,
        Generation generation,
        RefreshAttempt attempt,
        CommitEvidence evidence,
        ExecutionFence fence,
        CommandId commandId) {

    public PublishCommand {
        if (object == null || generation == null || attempt == null || evidence == null
                || fence == null || commandId == null) {
            throw new IllegalArgumentException("publication fields must not be null");
        }
        if (expectedObjectRevision < 0) {
            throw new IllegalArgumentException("expectedObjectRevision must not be negative");
        }
        if (!object.equals(generation.object()) || !object.equals(attempt.object())) {
            throw new IllegalArgumentException("publication entities belong to different objects");
        }
        if (!attempt.id().equals(attemptIdOf(generation))) {
            throw new IllegalArgumentException("generation was not produced by the publishing attempt");
        }
        if (attempt.evidence().isPresent() && !attempt.evidence().get().equals(evidence)) {
            throw new IllegalArgumentException("evidence does not match the attempt receipt");
        }
        if (generation.evidence().isPresent() && !generation.evidence().get().equals(evidence)) {
            throw new IllegalArgumentException("evidence does not match the generation receipt");
        }
    }

    private static AttemptId attemptIdOf(Generation generation) {
        return generation.producingAttempt();
    }
}
