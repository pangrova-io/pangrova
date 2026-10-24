package org.pangrova.materialization.domain;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;
import static org.assertj.core.api.Assertions.catchThrowableOfType;
import static org.pangrova.materialization.MaterializationTestFixtures.DEFINER;
import static org.pangrova.materialization.MaterializationTestFixtures.INCARNATION;
import static org.pangrova.materialization.MaterializationTestFixtures.LATER;
import static org.pangrova.materialization.MaterializationTestFixtures.NOW;
import static org.pangrova.materialization.MaterializationTestFixtures.REQUESTER;
import static org.pangrova.materialization.MaterializationTestFixtures.attempt;
import static org.pangrova.materialization.MaterializationTestFixtures.candidate;
import static org.pangrova.materialization.MaterializationTestFixtures.candidateGeneration;
import static org.pangrova.materialization.MaterializationTestFixtures.evidence;
import static org.pangrova.materialization.MaterializationTestFixtures.fence;
import static org.pangrova.materialization.MaterializationTestFixtures.objectId;
import static org.pangrova.materialization.MaterializationTestFixtures.source;
import static org.pangrova.materialization.MaterializationTestFixtures.vector;

import java.util.Optional;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.pangrova.common.exception.PangrovaErrorCode;
import org.pangrova.spi.materialization.CommitEvidence;

class GenerationAttemptLifecycleTest {

    private static final MaterializationId OBJECT = objectId("orders_dt");
    private static final GenerationId GENERATION = new GenerationId(
            UUID.fromString("00000000-0000-0000-0000-0000000000aa"));
    private static final AttemptId ATTEMPT = new AttemptId(
            UUID.fromString("00000000-0000-0000-0000-0000000000bb"));
    private static final SourceVector VECTOR = vector(source("orders"));
    private static final CommitEvidence EVIDENCE = evidence("commit-1");

    @Test
    void generationMovesCandidateCommittedPublishedRetiredCollected() {
        Generation candidate = fullGeneration();

        assertThat(candidate.state()).isEqualTo(GenerationState.Candidate.INSTANCE);
        assertThat(candidate.evidence()).isEmpty();
        assertThat(candidate.publishedAt()).isEmpty();

        Generation committed = candidate.commit(EVIDENCE, LATER);
        assertThat(committed.state()).isEqualTo(GenerationState.Committed.INSTANCE);
        assertThat(committed.evidence()).contains(EVIDENCE);
        assertThat(committed.publishedAt()).isEmpty();

        Generation published = committed.publish(LATER);
        assertThat(published.state()).isEqualTo(GenerationState.Published.INSTANCE);
        assertThat(published.publishedAt()).contains(LATER);

        Generation retired = published.retire();
        assertThat(retired.state()).isEqualTo(GenerationState.Retired.INSTANCE);
        assertThat(retired.publishedAt()).contains(LATER);

        Generation collected = retired.collect();
        assertThat(collected.state()).isEqualTo(GenerationState.Collected.INSTANCE);
    }

    @Test
    void candidateCannotPublishBeforeCommit() {
        Generation candidate = fullGeneration();

        MaterializationException exception = catchThrowableOfType(
                () -> candidate.publish(LATER), MaterializationException.class);

        assertThat(exception.getErrorCode())
                .isEqualTo(PangrovaErrorCode.MATERIALIZATION_INVALID_STATE_TRANSITION);
    }

    @Test
    void generationCannotCommitOrPublishTwice() {
        Generation committed = fullGeneration().commit(EVIDENCE, LATER);

        MaterializationException recommit = catchThrowableOfType(
                () -> committed.commit(EVIDENCE, LATER), MaterializationException.class);
        MaterializationException retireBeforePublish = catchThrowableOfType(
                () -> committed.retire(), MaterializationException.class);

        assertThat(recommit.getErrorCode())
                .isEqualTo(PangrovaErrorCode.MATERIALIZATION_INVALID_STATE_TRANSITION);
        assertThat(retireBeforePublish.getErrorCode())
                .isEqualTo(PangrovaErrorCode.MATERIALIZATION_INVALID_STATE_TRANSITION);
    }

    @Test
    void incrementalGenerationMustNameItsBaseAndFullMustNot() {
        GenerationId base = new GenerationId(
                UUID.fromString("00000000-0000-0000-0000-0000000000cc"));

        assertThatIllegalArgumentException()
                .isThrownBy(() -> candidateGeneration(
                        OBJECT, GENERATION, ATTEMPT, candidate(0),
                        RefreshKind.IncrementalAppendOnly.INSTANCE, VECTOR, Optional.empty()))
                .withMessageContaining("base generation");
        assertThatIllegalArgumentException()
                .isThrownBy(() -> candidateGeneration(
                        OBJECT, GENERATION, ATTEMPT, candidate(0),
                        RefreshKind.Full.INSTANCE, VECTOR, Optional.of(base)))
                .withMessageContaining("must not name a base generation");

        Generation incremental = candidateGeneration(
                OBJECT, GENERATION, ATTEMPT, candidate(0),
                RefreshKind.IncrementalAppendOnly.INSTANCE, VECTOR, Optional.of(base));
        assertThat(incremental.baseGeneration()).contains(base);
    }

    @Test
    void recordInvariantsRejectInconsistentPublications() {
        assertThatIllegalArgumentException()
                .isThrownBy(() -> new Generation(
                        GENERATION, OBJECT, INCARNATION, 1, ATTEMPT, Optional.empty(), candidate(0),
                        RefreshKind.Full.INSTANCE, VECTOR, Optional.empty(),
                        GenerationState.Committed.INSTANCE, NOW, Optional.empty()))
                .withMessageContaining("requires commit evidence");
        assertThatIllegalArgumentException()
                .isThrownBy(() -> new Generation(
                        GENERATION, OBJECT, INCARNATION, 1, ATTEMPT, Optional.empty(), candidate(0),
                        RefreshKind.Full.INSTANCE, VECTOR, Optional.of(EVIDENCE),
                        GenerationState.Published.INSTANCE, NOW, Optional.empty()))
                .withMessageContaining("publication instant");
        assertThatIllegalArgumentException()
                .isThrownBy(() -> new Generation(
                        GENERATION, OBJECT, INCARNATION, 1, ATTEMPT, Optional.empty(), candidate(0),
                        RefreshKind.Full.INSTANCE, VECTOR, Optional.empty(),
                        GenerationState.Candidate.INSTANCE, NOW, Optional.of(NOW)))
                .withMessageContaining("only published, retired, or collected");
    }

    @Test
    void quarantinedGenerationCanOnlyCommitOrBeCollected() {
        Generation quarantined = fullGeneration().quarantine();
        assertThat(quarantined.state()).isEqualTo(GenerationState.Quarantined.INSTANCE);

        Generation committed = quarantined.commit(EVIDENCE, LATER);
        assertThat(committed.state()).isEqualTo(GenerationState.Committed.INSTANCE);

        Generation collected = quarantined.collect();
        assertThat(collected.state()).isEqualTo(GenerationState.Collected.INSTANCE);

        MaterializationException exception = catchThrowableOfType(
                () -> quarantined.publish(LATER), MaterializationException.class);
        assertThat(exception.getErrorCode())
                .isEqualTo(PangrovaErrorCode.MATERIALIZATION_INVALID_STATE_TRANSITION);
    }

    @Test
    void attemptMovesPreparingBuildingCommittedPublished() {
        RefreshAttempt preparing = attempt(OBJECT, ATTEMPT, candidate(0), RefreshKind.Full.INSTANCE,
                VECTOR, Optional.empty());
        assertThat(preparing.state()).isEqualTo(AttemptState.Preparing.INSTANCE);
        assertThat(preparing.errorCode()).isEmpty();

        RefreshAttempt building = preparing.markBuilding(LATER);
        assertThat(building.state()).isEqualTo(AttemptState.Building.INSTANCE);

        RefreshAttempt committed = building.markCommitted(EVIDENCE, LATER);
        assertThat(committed.state()).isEqualTo(AttemptState.Committed.INSTANCE);
        assertThat(committed.evidence()).contains(EVIDENCE);

        RefreshAttempt published = committed.markPublished(LATER);
        assertThat(published.state()).isEqualTo(AttemptState.Published.INSTANCE);
        assertThat(published.evidence()).contains(EVIDENCE);
    }

    @Test
    void attemptIdentityAndCandidateAreImmutableAcrossTransitions() {
        RefreshAttempt preparing = attempt(OBJECT, ATTEMPT, candidate(7), RefreshKind.Full.INSTANCE,
                VECTOR, Optional.empty());
        RefreshAttempt building = preparing.markBuilding(LATER);

        assertThat(building.id()).isEqualTo(preparing.id());
        assertThat(building.candidateTable()).isEqualTo(preparing.candidateTable());
        assertThat(building.sourceVector()).isEqualTo(preparing.sourceVector());
        assertThat(building.definer()).isEqualTo(DEFINER);
        assertThat(building.requesting()).isEqualTo(REQUESTER);
        assertThat(building.fence()).isEqualTo(fence());
        assertThat(building.definitionRevision()).isEqualTo(1);
        assertThat(building.baseObjectRevision()).isZero();
        assertThat(building.incarnation()).isEqualTo(INCARNATION);
    }

    @Test
    void terminalAttemptStatesRejectFurtherTransitions() {
        RefreshAttempt failed = attempt(OBJECT, ATTEMPT, candidate(0), RefreshKind.Full.INSTANCE,
                VECTOR, Optional.empty()).fail("boom", LATER);

        assertThat(failed.state()).isEqualTo(AttemptState.Failed.INSTANCE);
        assertThat(failed.errorCode()).contains("boom");
        MaterializationException exception = catchThrowableOfType(
                () -> failed.markBuilding(LATER), MaterializationException.class);
        assertThat(exception.getErrorCode())
                .isEqualTo(PangrovaErrorCode.MATERIALIZATION_INVALID_STATE_TRANSITION);
    }

    @Test
    void failureRequiresAReason() {
        RefreshAttempt preparing = attempt(OBJECT, ATTEMPT, candidate(0), RefreshKind.Full.INSTANCE,
                VECTOR, Optional.empty());

        assertThatIllegalArgumentException()
                .isThrownBy(() -> preparing.fail(" ", LATER))
                .withMessageContaining("reason");
    }

    @Test
    void ambiguousAttemptIsQuarantinedAndRetainedForReconciliation() {
        RefreshAttempt building = attempt(OBJECT, ATTEMPT, candidate(3), RefreshKind.Full.INSTANCE,
                VECTOR, Optional.empty()).markBuilding(LATER);

        RefreshAttempt quarantined = building.quarantine("connector ack lost", LATER);
        assertThat(quarantined.state()).isEqualTo(AttemptState.Quarantined.INSTANCE);
        assertThat(quarantined.errorCode()).contains("connector ack lost");

        RefreshAttempt reconciled = quarantined.markCommitted(EVIDENCE, LATER);
        assertThat(reconciled.state()).isEqualTo(AttemptState.Committed.INSTANCE);
    }

    @Test
    void supersededAndAbandonedAttemptsAreTerminal() {
        RefreshAttempt committed = attempt(OBJECT, ATTEMPT, candidate(0), RefreshKind.Full.INSTANCE,
                VECTOR, Optional.empty()).markBuilding(LATER).markCommitted(EVIDENCE, LATER);
        RefreshAttempt superseded = committed.supersede("newer publication won", LATER);

        assertThat(superseded.state()).isEqualTo(AttemptState.Superseded.INSTANCE);
        assertThat(superseded.state().isTerminal()).isTrue();

        RefreshAttempt abandoned = attempt(OBJECT, ATTEMPT, candidate(0), RefreshKind.Full.INSTANCE,
                VECTOR, Optional.empty()).abandon("owner shut down", LATER);
        assertThat(abandoned.state()).isEqualTo(AttemptState.Abandoned.INSTANCE);
        assertThat(abandoned.state().isTerminal()).isTrue();
    }

    @Test
    void incrementalAttemptRequiresABaseGeneration() {
        GenerationId base = new GenerationId(
                UUID.fromString("00000000-0000-0000-0000-0000000000cc"));

        assertThatIllegalArgumentException()
                .isThrownBy(() -> attempt(OBJECT, ATTEMPT, candidate(0),
                        RefreshKind.IncrementalAppendOnly.INSTANCE, VECTOR, Optional.empty()))
                .withMessageContaining("base generation");
        assertThatIllegalArgumentException()
                .isThrownBy(() -> attempt(OBJECT, ATTEMPT, candidate(0),
                        RefreshKind.Full.INSTANCE, VECTOR, Optional.of(base)))
                .withMessageContaining("must not name a base generation");

        RefreshAttempt incremental = attempt(OBJECT, ATTEMPT, candidate(0),
                RefreshKind.IncrementalAppendOnly.INSTANCE, VECTOR, Optional.of(base));
        assertThat(incremental.baseGeneration()).contains(base);
    }

    private static Generation fullGeneration() {
        return candidateGeneration(OBJECT, GENERATION, ATTEMPT, candidate(0),
                RefreshKind.Full.INSTANCE, VECTOR, Optional.empty());
    }
}
