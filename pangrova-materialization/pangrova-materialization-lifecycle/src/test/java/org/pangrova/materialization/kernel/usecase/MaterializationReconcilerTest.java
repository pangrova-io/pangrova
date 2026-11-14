package org.pangrova.materialization.kernel.usecase;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.pangrova.materialization.kernel.InMemoryMaterializationRepository;
import org.pangrova.materialization.kernel.MaterializationTestFixtures;
import org.pangrova.materialization.api.enums.AttemptState;
import org.pangrova.materialization.api.domain.ExecutionFence;
import org.pangrova.materialization.api.domain.Generation;
import org.pangrova.materialization.api.domain.GenerationId;
import org.pangrova.materialization.api.enums.GenerationState;
import org.pangrova.materialization.api.domain.MaterializationId;
import org.pangrova.materialization.api.enums.MaterializationKind;
import org.pangrova.materialization.api.domain.MaterializationObject;
import org.pangrova.materialization.api.enums.ObjectState;
import org.pangrova.materialization.api.domain.PhysicalTableRef;
import org.pangrova.materialization.api.domain.RefreshAttempt;
import org.pangrova.materialization.api.enums.RefreshKind;
import org.pangrova.materialization.api.enums.RefreshMode;
import org.pangrova.materialization.api.port.Clock;
import org.pangrova.materialization.api.port.CommitRecovery;
import org.pangrova.materialization.api.port.FenceValidator;
import org.pangrova.materialization.api.port.LeaseService;
import org.pangrova.materialization.api.enums.MaterializationEventType;
import org.pangrova.materialization.api.port.PublicationVerification;
import org.pangrova.materialization.api.port.PublicationVerifier;
import org.pangrova.materialization.usecase.MaterializationReconciler;
import org.pangrova.materialization.usecase.ReconcileReport;

class MaterializationReconcilerTest {

    private static final Instant NOW = MaterializationTestFixtures.NOW;
    private static final Instant RECOVERY_NOW = NOW.plus(Duration.ofMinutes(30));
    private static final Instant TRANSITION_AT = NOW.plus(Duration.ofMinutes(5));
    private static final Duration GRACE = Duration.ofMinutes(5);
    private static final GenerationId OTHER_GENERATION =
            new GenerationId(UUID.fromString("00000000-0000-0000-0000-0000000000cc"));

    private MutableClock clock;
    private InMemoryMaterializationRepository repository;
    private ScriptedRecovery recovery;
    private RecordingVerifier verifier;
    private RecoveryFence fence;
    private MaterializationReconciler reconciler;
    private MaterializationObject object;

    @BeforeEach
    void setUp() {
        clock = new MutableClock(RECOVERY_NOW);
        repository = new InMemoryMaterializationRepository(clock);
        recovery = new ScriptedRecovery();
        verifier = new RecordingVerifier();
        fence = new RecoveryFence(clock);
        reconciler = new MaterializationReconciler(repository, recovery, verifier, fence, fence,
                clock, Duration.ofMinutes(30));
        object = MaterializationTestFixtures.creating(
                MaterializationKind.DYNAMIC_TABLE, RefreshMode.FULL);
        repository.putObject(object);
    }

    @Test
    void adoptsCommittedAttemptUnderAFreshLiveFence() {
        givenAttemptInState(committedAttempt(), committedGeneration());

        ReconcileReport report = reconciler.runOnce(GRACE);

        assertThat(report.examined()).isEqualTo(1);
        assertThat(report.adopted()).isEqualTo(1);
        MaterializationObject stored = storedObject();
        assertThat(stored.state()).isEqualTo(ObjectState.ACTIVE);
        assertThat(stored.revision()).isEqualTo(1);
        assertThat(stored.publishedGeneration()).contains(MaterializationTestFixtures.GENERATION_ID);
        assertThat(storedGeneration().state()).isEqualTo(GenerationState.PUBLISHED);
        assertThat(storedAttempt().state()).isEqualTo(AttemptState.PUBLISHED);
        assertThat(verifier.verified).hasSize(1);
        assertThat(fence.issued).isEqualTo(1);
        assertThat(repository.events())
                .extracting(event -> event.eventType())
                .contains(MaterializationEventType.ATTEMPT_RECONCILED);
    }

    @Test
    void recoversExactCommitEvidenceFromAPreparingAttempt() {
        RefreshAttempt preparing = MaterializationTestFixtures.attempt(
                object.id(), MaterializationTestFixtures.ATTEMPT_ID, candidateTable(),
                RefreshKind.FULL, MaterializationTestFixtures.vector(
                        MaterializationTestFixtures.source("orders")), Optional.empty());
        Generation candidate = candidateGeneration();
        givenAttemptInState(preparing, candidate);
        recovery.outcome = new CommitRecovery.Committed(MaterializationTestFixtures.evidence("c-2"));

        ReconcileReport report = reconciler.runOnce(GRACE);

        assertThat(report.adopted()).isEqualTo(1);
        assertThat(storedAttempt().state()).isEqualTo(AttemptState.PUBLISHED);
        assertThat(storedGeneration().state()).isEqualTo(GenerationState.PUBLISHED);
        assertThat(repository.findPublishedGeneration(object.id())).contains(storedGeneration());
    }

    @Test
    void abandonsAttemptProvenUncommitted() {
        givenAttemptInState(buildingAttempt(), candidateGeneration());
        recovery.outcome = new CommitRecovery.Uncommitted("connector reports no commit");

        ReconcileReport report = reconciler.runOnce(GRACE);

        assertThat(report.abandoned()).isEqualTo(1);
        assertThat(storedAttempt().state()).isEqualTo(AttemptState.ABANDONED);
        assertThat(storedGeneration().state()).isEqualTo(GenerationState.CANDIDATE);
        assertThat(storedObject().publishedGeneration()).isEmpty();
        assertThat(verifier.verified).isEmpty();
    }

    @Test
    void quarantinesAmbiguousCommitOutcome() {
        givenAttemptInState(buildingAttempt(), candidateGeneration());
        recovery.outcome = new CommitRecovery.Unknown("commit log unreachable");

        ReconcileReport report = reconciler.runOnce(GRACE);

        assertThat(report.quarantined()).isEqualTo(1);
        assertThat(storedAttempt().state()).isEqualTo(AttemptState.QUARANTINED);
        assertThat(storedGeneration().state()).isEqualTo(GenerationState.QUARANTINED);
        assertThat(storedObject().publishedGeneration()).isEmpty();
    }

    @Test
    void supersedesAttemptWhenANewerHeadWasPublishedFirst() {
        repository.putObject(object.publish(OTHER_GENERATION, 0, NOW));
        givenAttemptInState(committedAttempt(), committedGeneration());

        ReconcileReport report = reconciler.runOnce(GRACE);

        assertThat(report.superseded()).isEqualTo(1);
        assertThat(storedAttempt().state()).isEqualTo(AttemptState.SUPERSEDED);
        assertThat(storedGeneration().state()).isEqualTo(GenerationState.COMMITTED);
        MaterializationObject stored = storedObject();
        assertThat(stored.revision()).isEqualTo(1);
        assertThat(stored.publishedGeneration()).contains(OTHER_GENERATION);
        assertThat(verifier.verified).isEmpty();
    }

    @Test
    void defersWhenRecoveryCannotBeDetermined() {
        RefreshAttempt building = buildingAttempt();
        givenAttemptInState(building, candidateGeneration());
        recovery.failure = new IllegalStateException("connector unavailable");

        ReconcileReport report = reconciler.runOnce(GRACE);

        assertThat(report.failed()).isEqualTo(1);
        assertThat(storedAttempt().state()).isEqualTo(AttemptState.BUILDING);
        assertThat(storedGeneration().state()).isEqualTo(GenerationState.CANDIDATE);
    }

    @Test
    void adoptsQuarantinedAttemptOnceExactEvidenceIsRecovered() {
        RefreshAttempt quarantined = buildingAttempt()
                .quarantine("ambiguous execution", TRANSITION_AT);
        givenAttemptInState(quarantined, candidateGeneration().quarantine());
        recovery.outcome = new CommitRecovery.Committed(MaterializationTestFixtures.evidence("c-3"));

        ReconcileReport report = reconciler.runOnce(GRACE);

        assertThat(report.adopted()).isEqualTo(1);
        assertThat(storedAttempt().state()).isEqualTo(AttemptState.PUBLISHED);
        assertThat(storedGeneration().state()).isEqualTo(GenerationState.PUBLISHED);
    }

    @Test
    void leavesAttemptsWhoseExecutionAnchorIsStillLive() {
        givenAttemptInState(committedAttempt(), committedGeneration());
        repository.putLiveAnchor(MaterializationTestFixtures.fence().executionAnchor());

        ReconcileReport report = reconciler.runOnce(GRACE);

        assertThat(report.examined()).isZero();
        assertThat(storedObject().publishedGeneration()).isEmpty();
        assertThat(storedAttempt().state()).isEqualTo(AttemptState.COMMITTED);
    }

    @Test
    void leavesQuarantinedAttemptInsideTheGraceWindowWhileTheAnchorLives() {
        RefreshAttempt quarantined = buildingAttempt().quarantine("ambiguous execution",
                RECOVERY_NOW.minus(Duration.ofMinutes(1)));
        givenAttemptInState(quarantined, candidateGeneration().quarantine());
        repository.putLiveAnchor(MaterializationTestFixtures.fence().executionAnchor());
        recovery.outcome = new CommitRecovery.Committed(MaterializationTestFixtures.evidence("c-4"));

        ReconcileReport report = reconciler.runOnce(GRACE);

        assertThat(report.examined()).isZero();
        assertThat(storedAttempt().state()).isEqualTo(AttemptState.QUARANTINED);
        assertThat(storedGeneration().state()).isEqualTo(GenerationState.QUARANTINED);
        assertThat(fence.issued).isZero();
    }

    private void givenAttemptInState(RefreshAttempt attempt, Generation generation) {
        repository.putAttempt(attempt);
        repository.putGeneration(generation);
    }

    private RefreshAttempt buildingAttempt() {
        return MaterializationTestFixtures.attempt(
                object.id(), MaterializationTestFixtures.ATTEMPT_ID, candidateTable(),
                RefreshKind.FULL, MaterializationTestFixtures.vector(
                        MaterializationTestFixtures.source("orders")), Optional.empty())
                .markBuilding(TRANSITION_AT);
    }

    private RefreshAttempt committedAttempt() {
        return buildingAttempt().markCommitted(
                MaterializationTestFixtures.evidence("c-1"), TRANSITION_AT);
    }

    private Generation committedGeneration() {
        return candidateGeneration().commit(
                MaterializationTestFixtures.evidence("c-1"), TRANSITION_AT);
    }

    private Generation candidateGeneration() {
        return MaterializationTestFixtures.candidateGeneration(
                object.id(), MaterializationTestFixtures.GENERATION_ID,
                MaterializationTestFixtures.ATTEMPT_ID, candidateTable(),
                RefreshKind.FULL, MaterializationTestFixtures.vector(
                        MaterializationTestFixtures.source("orders")), Optional.empty());
    }

    private static PhysicalTableRef candidateTable() {
        return MaterializationTestFixtures.candidate(0);
    }

    private MaterializationObject storedObject() {
        return repository.storedObject(object.id()).orElseThrow();
    }

    private RefreshAttempt storedAttempt() {
        return repository.findAttempt(MaterializationTestFixtures.ATTEMPT_ID).orElseThrow();
    }

    private Generation storedGeneration() {
        return repository.findGeneration(MaterializationTestFixtures.GENERATION_ID).orElseThrow();
    }

    private static final class ScriptedRecovery implements CommitRecovery {

        private CommitRecovery.Outcome outcome;
        private RuntimeException failure;

        @Override
        public Outcome recover(RefreshAttempt attempt) {
            if (failure != null) {
                throw failure;
            }
            if (outcome == null) {
                throw new IllegalStateException("no scripted recovery outcome");
            }
            return outcome;
        }
    }

    private static final class RecordingVerifier implements PublicationVerifier {

        private final List<PublicationVerification> verified = new ArrayList<>();

        @Override
        public void verify(PublicationVerification verification) {
            verified.add(verification);
        }
    }

    private static final class RecoveryFence implements LeaseService, FenceValidator {

        private final Clock clock;
        private int issued;

        private RecoveryFence(Clock clock) {
            this.clock = clock;
        }

        @Override
        public ExecutionFence issueExecutionFence(MaterializationId object,
                Duration leaseDuration) {
            issued++;
            return new ExecutionFence("recovery-anchor-" + issued, 9,
                    clock.now().plus(leaseDuration));
        }

        @Override
        public void requireValid(ExecutionFence fence, Instant now) {
            fence.requireValidAt(now);
        }
    }

    private static final class MutableClock implements Clock {

        private final Instant now;

        private MutableClock(Instant now) {
            this.now = now;
        }

        @Override
        public Instant now() {
            return now;
        }
    }
}
