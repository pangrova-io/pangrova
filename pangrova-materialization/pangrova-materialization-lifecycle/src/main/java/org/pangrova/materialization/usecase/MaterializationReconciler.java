package org.pangrova.materialization.usecase;

import org.pangrova.materialization.enums.ReconciliationOutcome;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.Optional;
import java.util.UUID;
import org.pangrova.materialization.api.enums.AttemptState;
import org.pangrova.materialization.api.domain.CommandId;
import org.pangrova.materialization.api.domain.ExecutionFence;
import org.pangrova.materialization.api.domain.Generation;
import org.pangrova.materialization.api.exception.MaterializationException;
import org.pangrova.materialization.api.domain.MaterializationObject;
import org.pangrova.materialization.api.enums.ObjectState;
import org.pangrova.materialization.api.domain.RefreshAttempt;
import org.pangrova.materialization.api.port.Clock;
import org.pangrova.materialization.api.port.CommitRecovery;
import org.pangrova.materialization.api.port.FenceValidator;
import org.pangrova.materialization.api.port.LeaseService;
import org.pangrova.materialization.api.port.MaterializationEvent;
import org.pangrova.materialization.api.enums.MaterializationEventType;
import org.pangrova.persistence.materialization.api.lifecycle.MaterializationRepository;
import org.pangrova.materialization.api.port.PublicationVerification;
import org.pangrova.materialization.api.port.PublicationVerifier;
import org.pangrova.materialization.api.port.PublishCommand;
import org.pangrova.connector.api.materialization.CommitEvidence;

/**
 * Reconciles attempts left unsettled by a crash, lost acknowledgement, or
 * leader change.
 *
 * <p>Recovery never re-runs the data plane. An attempt with exact evidence is
 * published under a fresh live fence; an attempt proven uncommitted is
 * abandoned; every weaker observation is quarantined and retained. A newer
 * head or definition revision supersedes the attempt so its candidate becomes
 * eligible for garbage collection.
 */
public final class MaterializationReconciler {

    private final MaterializationRepository repository;
    private final CommitRecovery recovery;
    private final PublicationVerifier verifier;
    private final LeaseService leases;
    private final FenceValidator fences;
    private final Clock clock;
    private final Duration recoveryLease;

    /**
     * Creates a reconciler over the durable attempt and publication records.
     *
     * @param repository durable repository
     * @param recovery commit recovery probe of the execution connector
     * @param verifier publication verifier
     * @param leases execution lease issuer
     * @param fences execution fence validator
     * @param clock time source
     * @param recoveryLease lease issued while adopting a recovered attempt
     */
    public MaterializationReconciler(MaterializationRepository repository,
            CommitRecovery recovery, PublicationVerifier verifier, LeaseService leases,
            FenceValidator fences, Clock clock, Duration recoveryLease) {
        if (repository == null || recovery == null || verifier == null || leases == null
                || fences == null || clock == null) {
            throw new IllegalArgumentException("reconciler collaborators must not be null");
        }
        if (recoveryLease == null || recoveryLease.isZero() || recoveryLease.isNegative()) {
            throw new IllegalArgumentException("recoveryLease must be positive");
        }
        this.repository = repository;
        this.recovery = recovery;
        this.verifier = verifier;
        this.leases = leases;
        this.fences = fences;
        this.clock = clock;
        this.recoveryLease = recoveryLease;
    }

    /**
     * Reconciles every attempt whose execution anchor is no longer live, plus
     * quarantined attempts observed for longer than the grace period.
     *
     * @param grace minimum observation period of a quarantined attempt
     * @return reconciliation counters
     */
    public ReconcileReport runOnce(Duration grace) {
        if (grace == null || grace.isNegative()) {
            throw new IllegalArgumentException("grace must not be negative");
        }
        Instant now = clock.now();
        int examined = 0;
        int adopted = 0;
        int superseded = 0;
        int abandoned = 0;
        int quarantined = 0;
        int failed = 0;
        for (RefreshAttempt attempt : repository.findReconcilableAttempts(now.minus(grace))) {
            examined++;
            Optional<Generation> generation = repository.findGenerationByAttempt(attempt.id());
            if (generation.isEmpty()) {
                failed++;
                continue;
            }
            try {
                ReconciliationOutcome kind = reconcile(attempt, generation.orElseThrow(), now);
                switch (kind) {
                    case ADOPTED -> adopted++;
                    case SUPERSEDED -> superseded++;
                    case ABANDONED -> abandoned++;
                    case QUARANTINED -> quarantined++;
                    case DEFERRED -> failed++;
                    default -> throw new IllegalStateException("Unhandled reconcile outcome: " + kind);
                }
            } catch (RuntimeException failure) {
                failed++;
            }
        }
        return new ReconcileReport(examined, adopted, superseded, abandoned, quarantined, failed);
    }

    private ReconciliationOutcome reconcile(RefreshAttempt attempt, Generation generation, Instant now) {
        if (attempt.state() == AttemptState.COMMITTED) {
            return publish(attempt, generation, now) ? ReconciliationOutcome.ADOPTED : ReconciliationOutcome.SUPERSEDED;
        }
        if (attempt.state() == AttemptState.QUARANTINED) {
            return reconcileQuarantined(attempt, generation, now);
        }
        CommitRecovery.Outcome outcome;
        try {
            outcome = recovery.recover(attempt);
        } catch (RuntimeException failure) {
            return ReconciliationOutcome.DEFERRED;
        }
        if (outcome instanceof CommitRecovery.Committed committed) {
            repository.recordCommitEvidence(attempt.id(), committed.evidence(), now);
            RefreshAttempt recorded = repository.findAttempt(attempt.id())
                    .orElseThrow(() -> MaterializationException.repositoryFailure(
                            "recovered attempt disappeared: " + attempt.id().value(), null));
            Generation committedGeneration = repository.findGenerationByAttempt(attempt.id())
                    .orElseThrow(() -> MaterializationException.repositoryFailure(
                            "recovered generation disappeared: " + attempt.id().value(), null));
            return publish(recorded, committedGeneration, now)
                    ? ReconciliationOutcome.ADOPTED : ReconciliationOutcome.SUPERSEDED;
        }
        if (outcome instanceof CommitRecovery.Uncommitted uncommitted) {
            abandon(attempt, generation, uncommitted.reason(), now);
            return ReconciliationOutcome.ABANDONED;
        }
        CommitRecovery.Unknown unknown = (CommitRecovery.Unknown) outcome;
        quarantine(attempt, generation, "reconcile: " + unknown.reason(), now);
        return ReconciliationOutcome.QUARANTINED;
    }

    private ReconciliationOutcome reconcileQuarantined(RefreshAttempt attempt, Generation generation,
            Instant now) {
        CommitRecovery.Outcome outcome;
        try {
            outcome = recovery.recover(attempt);
        } catch (RuntimeException failure) {
            return ReconciliationOutcome.DEFERRED;
        }
        if (outcome instanceof CommitRecovery.Committed committed) {
            repository.recordCommitEvidence(attempt.id(), committed.evidence(), now);
            RefreshAttempt recorded = repository.findAttempt(attempt.id())
                    .orElseThrow(() -> MaterializationException.repositoryFailure(
                            "recovered attempt disappeared: " + attempt.id().value(), null));
            Generation committedGeneration = repository.findGenerationByAttempt(attempt.id())
                    .orElseThrow(() -> MaterializationException.repositoryFailure(
                            "recovered generation disappeared: " + attempt.id().value(), null));
            return publish(recorded, committedGeneration, now)
                    ? ReconciliationOutcome.ADOPTED : ReconciliationOutcome.SUPERSEDED;
        }
        if (outcome instanceof CommitRecovery.Uncommitted uncommitted) {
            abandon(attempt, generation, uncommitted.reason(), now);
            return ReconciliationOutcome.ABANDONED;
        }
        return ReconciliationOutcome.QUARANTINED;
    }

    private boolean publish(RefreshAttempt attempt, Generation generation, Instant now) {
        MaterializationObject header = repository.findObject(attempt.object())
                .orElseThrow(() -> MaterializationException.notFound(attempt.object().canonicalName()));
        boolean headAdvanced = header.publishedGeneration()
                .filter(published -> !published.equals(generation.id()))
                .isPresent();
        boolean incompatible = header.definitionRevision() != generation.definitionRevision()
                || !header.incarnation().equals(generation.incarnation())
                || header.state() == ObjectState.DROPPING
                || header.state() == ObjectState.DROPPED;
        if (headAdvanced || incompatible) {
            supersede(attempt, generation, now);
            return false;
        }
        CommitEvidence evidence = attempt.evidence()
                .orElseThrow(() -> MaterializationException.evidenceMismatch(
                        "attempt " + attempt.id().value() + " has no commit evidence"));
        ExecutionFence fence = leases.issueExecutionFence(attempt.object(), recoveryLease);
        fences.requireValid(fence, now);
        verifier.verify(new PublicationVerification(generation, attempt, evidence));
        repository.publish(new PublishCommand(attempt.object(), header.revision(),
                generation, attempt, evidence, fence, commandId(attempt)),
                event(MaterializationEventType.ATTEMPT_RECONCILED, attempt,
                        "adopted committed attempt " + attempt.id().value(), now));
        return true;
    }

    private void supersede(RefreshAttempt attempt, Generation generation, Instant now) {
        repository.settleAttempt(attempt, attempt.supersede("newer head or definition", now),
                generation, event(MaterializationEventType.ATTEMPT_RECONCILED, attempt,
                        "superseded by newer head or definition", now));
    }

    private void abandon(RefreshAttempt attempt, Generation generation, String reason, Instant now) {
        repository.settleAttempt(attempt, attempt.abandon("reconcile: " + reason, now), generation,
                event(MaterializationEventType.ATTEMPT_RECONCILED, attempt,
                        "abandoned uncommitted attempt: " + reason, now));
    }

    private void quarantine(RefreshAttempt attempt, Generation generation, String reason,
            Instant now) {
        RefreshAttempt quarantined = attempt.quarantine(reason, now);
        repository.settleAttempt(attempt, quarantined, generation.quarantine(),
                event(MaterializationEventType.ATTEMPT_QUARANTINED, attempt, reason, now));
    }

    private static CommandId commandId(RefreshAttempt attempt) {
        return new CommandId("rec-" + UUID.nameUUIDFromBytes(
                attempt.id().value().toString().getBytes(StandardCharsets.UTF_8)));
    }

    private static MaterializationEvent event(MaterializationEventType type,
            RefreshAttempt attempt, String detail, Instant now) {
        return new MaterializationEvent(UUID.randomUUID().toString(), type, attempt.object(),
                Optional.empty(), Optional.of(attempt.id()), Optional.empty(), Optional.empty(),
                attempt.requesting(), attempt.definer(), detail, now);
    }

}
