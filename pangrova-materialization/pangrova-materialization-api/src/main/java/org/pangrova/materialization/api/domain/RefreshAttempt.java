package org.pangrova.materialization.api.domain;

import org.pangrova.materialization.api.enums.RefreshKind;
import org.pangrova.materialization.api.enums.AttemptState;
import org.pangrova.materialization.api.enums.TriggerKind;
import org.pangrova.materialization.api.exception.MaterializationException;

import java.time.Instant;
import java.util.Optional;
import org.pangrova.connector.api.materialization.CommitEvidence;

import static org.pangrova.common.util.Preconditions.requireNonNullField;
import static org.pangrova.common.util.Preconditions.requireText;

/**
 * One durable refresh attempt.
 *
 * <p>An attempt is reserved before any data-plane I/O and binds a single
 * candidate table, source vector, execution fence, requesting principal, and
 * definer. Its identity and candidate are immutable; retries create new
 * attempts.
 *
 * @param id attempt identity
 * @param object owning object
 * @param incarnation object incarnation
 * @param definitionRevision definition revision being executed
 * @param baseObjectRevision object head revision observed at reservation
 * @param baseGeneration base generation for incremental kinds
 * @param refreshKind resolved refresh kind
 * @param sourceVector exact source snapshots
 * @param trigger attempt origin
 * @param requesting principal that requested the refresh
 * @param definer principal whose authority the result is computed under
 * @param fence execution fence
 * @param candidateTable reserved physical candidate table
 * @param state attempt state
 * @param evidence exact commit evidence once observed
 * @param errorCode structured failure or quarantine reason
 * @param createdAt reservation instant
 * @param updatedAt last transition instant
 */
public record RefreshAttempt(
        AttemptId id,
        MaterializationId object,
        IncarnationId incarnation,
        long definitionRevision,
        long baseObjectRevision,
        Optional<GenerationId> baseGeneration,
        RefreshKind refreshKind,
        SourceVector sourceVector,
        TriggerKind trigger,
        Principal requesting,
        Principal definer,
        ExecutionFence fence,
        PhysicalTableRef candidateTable,
        AttemptState state,
        Optional<CommitEvidence> evidence,
        Optional<String> errorCode,
        Instant createdAt,
        Instant updatedAt) {

    public RefreshAttempt {
        requireIdentity(id, object, incarnation);
        requireRevision(definitionRevision, baseObjectRevision);
        baseGeneration = requireNonNullField(baseGeneration, "baseGeneration");
        requireScope(refreshKind, sourceVector, trigger, requesting, definer, fence, candidateTable);
        if (state == null) {
            throw new IllegalArgumentException("state must not be null");
        }
        evidence = requireNonNullField(evidence, "evidence");
        errorCode = requireNonNullField(errorCode, "errorCode");
        requireBaseConsistency(refreshKind, baseGeneration);
        requireStateEvidence(state, evidence, errorCode);
        requireTimestamps(createdAt, updatedAt);
    }

    private static void requireIdentity(AttemptId id, MaterializationId object,
            IncarnationId incarnation) {
        if (id == null || object == null || incarnation == null) {
            throw new IllegalArgumentException("id, object, and incarnation must not be null");
        }
    }

    private static void requireRevision(long definitionRevision, long baseObjectRevision) {
        if (definitionRevision < 1) {
            throw new IllegalArgumentException("definitionRevision must be positive");
        }
        if (baseObjectRevision < 0) {
            throw new IllegalArgumentException("baseObjectRevision must not be negative");
        }
    }

    private static void requireScope(RefreshKind refreshKind, SourceVector sourceVector,
            TriggerKind trigger, Principal requesting, Principal definer, ExecutionFence fence,
            PhysicalTableRef candidateTable) {
        if (refreshKind == null || sourceVector == null || trigger == null) {
            throw new IllegalArgumentException("refreshKind, sourceVector, and trigger are required");
        }
        if (requesting == null || definer == null || fence == null || candidateTable == null) {
            throw new IllegalArgumentException(
                    "requesting, definer, fence, and candidateTable are required");
        }
    }

    private static void requireBaseConsistency(RefreshKind refreshKind,
            Optional<GenerationId> baseGeneration) {
        if (refreshKind.isIncremental() && baseGeneration.isEmpty()) {
            throw new IllegalArgumentException("incremental attempt must name a base generation");
        }
        if (!refreshKind.isIncremental() && baseGeneration.isPresent()) {
            throw new IllegalArgumentException("full attempt must not name a base generation");
        }
    }

    private static void requireStateEvidence(AttemptState state,
            Optional<CommitEvidence> attemptEvidence, Optional<String> attemptErrorCode) {
        if ((state == AttemptState.COMMITTED || state == AttemptState.PUBLISHED)
                && attemptEvidence.isEmpty()) {
            throw new IllegalArgumentException("committed or published attempt requires commit evidence");
        }
        if (state == AttemptState.FAILED && attemptErrorCode.isEmpty()) {
            throw new IllegalArgumentException("failed attempt requires an error code");
        }
    }

    private static void requireTimestamps(Instant attemptCreatedAt, Instant attemptUpdatedAt) {
        if (attemptCreatedAt == null || attemptUpdatedAt == null) {
            throw new IllegalArgumentException("createdAt and updatedAt must not be null");
        }
        if (attemptUpdatedAt.isBefore(attemptCreatedAt)) {
            throw new IllegalArgumentException("updatedAt must not precede createdAt");
        }
    }

    /**
     * Creates a durable reservation in the preparing state.
     *
     * <p>The reservation snapshots the object header and the definition revision
     * it executes against, so a later header or definition change can never
     * retroactively alter what this attempt reserved.
     *
     * @param id attempt identity
     * @param header object header observed at reservation
     * @param definition definition revision being executed
     * @param requesting principal that requested the refresh
     * @param scope execution scope of the attempt
     * @param now reservation instant
     * @return preparing attempt
     */
    public static RefreshAttempt reserve(AttemptId id, MaterializationObject header,
            DefinitionRevision definition, Principal requesting, AttemptScope scope,
            Instant now) {
        return new RefreshAttempt(id, header.id(), header.incarnation(), definition.revision(),
                header.revision(), scope.baseGeneration(), scope.refreshKind(),
                scope.sourceVector(), scope.trigger(), requesting, definition.definer(),
                scope.fence(), scope.candidateTable(), AttemptState.PREPARING,
                Optional.empty(), Optional.empty(), now, now);
    }

    /**
     * Execution scope of a reservation.
     *
     * @param baseGeneration base generation for incremental kinds
     * @param refreshKind resolved refresh kind
     * @param sourceVector exact source snapshots
     * @param trigger attempt origin
     * @param fence execution fence
     * @param candidateTable reserved candidate table
     */
    public record AttemptScope(
            Optional<GenerationId> baseGeneration,
            RefreshKind refreshKind,
            SourceVector sourceVector,
            TriggerKind trigger,
            ExecutionFence fence,
            PhysicalTableRef candidateTable) {
    }

    /**
     * Marks the attempt as executing in the data plane.
     *
     * @param now transition instant
     * @return building attempt
     */
    public RefreshAttempt markBuilding(Instant now) {
        requireTransition(AttemptState.BUILDING);
        return withState(AttemptState.BUILDING, evidence, errorCode, now);
    }

    /**
     * Records exact commit evidence and moves the attempt to committed.
     *
     * @param commitEvidence exact connector evidence
     * @param now transition instant
     * @return committed attempt
     */
    public RefreshAttempt markCommitted(CommitEvidence commitEvidence, Instant now) {
        requireTransition(AttemptState.COMMITTED);
        if (evidence.isPresent()) {
            throw MaterializationException.evidenceMismatch("attempt already has commit evidence");
        }
        return withState(AttemptState.COMMITTED, Optional.of(commitEvidence), errorCode, now);
    }

    /**
     * Marks the attempt published after the object head advanced.
     *
     * @param now transition instant
     * @return published attempt
     */
    public RefreshAttempt markPublished(Instant now) {
        requireTransition(AttemptState.PUBLISHED);
        if (evidence.isEmpty()) {
            throw MaterializationException.evidenceMismatch(
                    "cannot publish attempt " + id + " without commit evidence");
        }
        return withState(AttemptState.PUBLISHED, evidence, errorCode, now);
    }

    /**
     * Marks the attempt failed before a durable commit was observed.
     *
     * @param reason structured failure reason
     * @param now transition instant
     * @return failed attempt
     */
    public RefreshAttempt fail(String reason, Instant now) {
        requireTransition(AttemptState.FAILED);
        return withState(AttemptState.FAILED, evidence, Optional.of(requireText(reason, "reason")), now);
    }

    /**
     * Marks the attempt abandoned by its owner.
     *
     * @param reason structured abandonment reason
     * @param now transition instant
     * @return abandoned attempt
     */
    public RefreshAttempt abandon(String reason, Instant now) {
        requireTransition(AttemptState.ABANDONED);
        return withState(AttemptState.ABANDONED, evidence, Optional.of(requireText(reason, "reason")), now);
    }

    /**
     * Quarantines an attempt whose commit outcome is ambiguous.
     *
     * @param reason ambiguity detail
     * @param now transition instant
     * @return quarantined attempt
     */
    public RefreshAttempt quarantine(String reason, Instant now) {
        requireTransition(AttemptState.QUARANTINED);
        return withState(AttemptState.QUARANTINED, evidence, Optional.of(requireText(reason, "reason")), now);
    }

    /**
     * Marks the attempt superseded by a newer publication.
     *
     * @param reason supersession detail
     * @param now transition instant
     * @return superseded attempt
     */
    public RefreshAttempt supersede(String reason, Instant now) {
        requireTransition(AttemptState.SUPERSEDED);
        return withState(AttemptState.SUPERSEDED, evidence, Optional.of(requireText(reason, "reason")), now);
    }

    private RefreshAttempt withState(
            AttemptState target, Optional<CommitEvidence> newEvidence, Optional<String> newError, Instant now) {
        return new RefreshAttempt(id, object, incarnation, definitionRevision, baseObjectRevision,
                baseGeneration, refreshKind, sourceVector, trigger, requesting, definer, fence,
                candidateTable, target, newEvidence, newError, createdAt, now);
    }

    private void requireTransition(AttemptState target) {
        if (!state.canTransitionTo(target)) {
            throw MaterializationException.invalidStateTransition(
                    "attempt " + id + " cannot transition from " + state + " to " + target);
        }
    }
}
