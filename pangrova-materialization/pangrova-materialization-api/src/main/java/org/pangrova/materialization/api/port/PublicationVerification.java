package org.pangrova.materialization.api.port;

import org.pangrova.materialization.api.domain.Generation;
import org.pangrova.materialization.api.domain.RefreshAttempt;
import org.pangrova.connector.api.materialization.CommitEvidence;

/**
 * Verification input for a publication decision.
 *
 * @param generation candidate generation
 * @param attempt attempt that produced it
 * @param evidence exact commit evidence observed for the candidate
 */
public record PublicationVerification(Generation generation, RefreshAttempt attempt, CommitEvidence evidence) {

    public PublicationVerification {
        if (generation == null || attempt == null || evidence == null) {
            throw new IllegalArgumentException("generation, attempt, and evidence must not be null");
        }
        if (!generation.object().equals(attempt.object())) {
            throw new IllegalArgumentException("generation and attempt belong to different objects");
        }
        if (generation.definitionRevision() != attempt.definitionRevision()) {
            throw new IllegalArgumentException("generation and attempt revisions differ");
        }
        if (!generation.physicalTable().sameTable(attempt.candidateTable())) {
            throw new IllegalArgumentException("generation is not stored in the attempt candidate");
        }
        if (attempt.evidence().isPresent() && !attempt.evidence().get().equals(evidence)) {
            throw new IllegalArgumentException("evidence does not match the attempt receipt");
        }
        if (generation.evidence().isPresent() && !generation.evidence().get().equals(evidence)) {
            throw new IllegalArgumentException("evidence does not match the generation receipt");
        }
    }
}
