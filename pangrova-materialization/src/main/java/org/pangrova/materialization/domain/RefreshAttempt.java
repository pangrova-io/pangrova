package org.pangrova.materialization.domain;

import java.time.Instant;
import java.util.Optional;
import org.pangrova.spi.materialization.CommitEvidence;

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
        if (id == null || object == null || incarnation == null) {
            throw new IllegalArgumentException("id, object, and incarnation must not be null");
        }
        if (definitionRevision < 1) {
            throw new IllegalArgumentException("definitionRevision must be positive");
        }
        if (baseObjectRevision < 0) {
            throw new IllegalArgumentException("baseObjectRevision must not be negative");
        }
        baseGeneration = Require.notNull(baseGeneration, "baseGeneration");
        if (refreshKind == null || sourceVector == null || trigger == null) {
            throw new IllegalArgumentException("refreshKind, sourceVector, and trigger are required");
        }
        if (requesting == null || definer == null || fence == null || candidateTable == null) {
            throw new IllegalArgumentException("requesting, definer, fence, and candidateTable are required");
        }
        if (state == null) {
            throw new IllegalArgumentException("state must not be null");
        }
        evidence = Require.notNull(evidence, "evidence");
        errorCode = Require.notNull(errorCode, "errorCode");
        if (refreshKind.isIncremental() && baseGeneration.isEmpty()) {
            throw new IllegalArgumentException("incremental attempt must name a base generation");
        }
        if (!refreshKind.isIncremental() && baseGeneration.isPresent()) {
            throw new IllegalArgumentException("full attempt must not name a base generation");
        }
        if ((state == AttemptState.Committed.INSTANCE || state == AttemptState.Published.INSTANCE)
                && evidence.isEmpty()) {
            throw new IllegalArgumentException("committed or published attempt requires commit evidence");
        }
        if (state == AttemptState.Failed.INSTANCE && errorCode.isEmpty()) {
            throw new IllegalArgumentException("failed attempt requires an error code");
        }
        if (createdAt == null || updatedAt == null) {
            throw new IllegalArgumentException("createdAt and updatedAt must not be null");
        }
        if (updatedAt.isBefore(createdAt)) {
            throw new IllegalArgumentException("updatedAt must not precede createdAt");
        }
    }

    /**
     * Creates a durable reservation in the preparing state.
     *
     * @param id attempt identity
     * @param object owning object
     * @param incarnation object incarnation
     * @param definitionRevision definition revision
     * @param baseObjectRevision observed head revision
     * @param baseGeneration base generation for incremental kinds
     * @param refreshKind resolved refresh kind
     * @param sourceVector exact source snapshots
     * @param trigger attempt origin
     * @param requesting requesting principal
     * @param definer definer principal
     * @param fence execution fence
     * @param candidateTable reserved candidate table
     * @param now reservation instant
     * @return preparing attempt
     */
    public static RefreshAttempt reserve(
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
            Instant now) {
        return new RefreshAttempt(id, object, incarnation, definitionRevision, baseObjectRevision,
                baseGeneration, refreshKind, sourceVector, trigger, requesting, definer, fence,
                candidateTable, AttemptState.Preparing.INSTANCE, Optional.empty(), Optional.empty(),
                now, now);
    }

    /**
     * Marks the attempt as executing in the data plane.
     *
     * @param now transition instant
     * @return building attempt
     */
    public RefreshAttempt markBuilding(Instant now) {
        requireTransition(AttemptState.Building.INSTANCE);
        return withState(AttemptState.Building.INSTANCE, evidence, errorCode, now);
    }

    /**
     * Records exact commit evidence and moves the attempt to committed.
     *
     * @param commitEvidence exact connector evidence
     * @param now transition instant
     * @return committed attempt
     */
    public RefreshAttempt markCommitted(CommitEvidence commitEvidence, Instant now) {
        requireTransition(AttemptState.Committed.INSTANCE);
        if (evidence.isPresent()) {
            throw MaterializationException.evidenceMismatch("attempt already has commit evidence");
        }
        return withState(AttemptState.Committed.INSTANCE, Optional.of(commitEvidence), errorCode, now);
    }

    /**
     * Marks the attempt published after the object head advanced.
     *
     * @param now transition instant
     * @return published attempt
     */
    public RefreshAttempt markPublished(Instant now) {
        requireTransition(AttemptState.Published.INSTANCE);
        if (evidence.isEmpty()) {
            throw MaterializationException.evidenceMismatch(
                    "cannot publish attempt " + id + " without commit evidence");
        }
        return withState(AttemptState.Published.INSTANCE, evidence, errorCode, now);
    }

    /**
     * Marks the attempt failed before a durable commit was observed.
     *
     * @param reason structured failure reason
     * @param now transition instant
     * @return failed attempt
     */
    public RefreshAttempt fail(String reason, Instant now) {
        requireTransition(AttemptState.Failed.INSTANCE);
        return withState(AttemptState.Failed.INSTANCE, evidence, Optional.of(Require.text(reason, "reason")), now);
    }

    /**
     * Marks the attempt abandoned by its owner.
     *
     * @param reason structured abandonment reason
     * @param now transition instant
     * @return abandoned attempt
     */
    public RefreshAttempt abandon(String reason, Instant now) {
        requireTransition(AttemptState.Abandoned.INSTANCE);
        return withState(AttemptState.Abandoned.INSTANCE, evidence, Optional.of(Require.text(reason, "reason")), now);
    }

    /**
     * Quarantines an attempt whose commit outcome is ambiguous.
     *
     * @param reason ambiguity detail
     * @param now transition instant
     * @return quarantined attempt
     */
    public RefreshAttempt quarantine(String reason, Instant now) {
        requireTransition(AttemptState.Quarantined.INSTANCE);
        return withState(AttemptState.Quarantined.INSTANCE, evidence, Optional.of(Require.text(reason, "reason")), now);
    }

    /**
     * Marks the attempt superseded by a newer publication.
     *
     * @param reason supersession detail
     * @param now transition instant
     * @return superseded attempt
     */
    public RefreshAttempt supersede(String reason, Instant now) {
        requireTransition(AttemptState.Superseded.INSTANCE);
        return withState(AttemptState.Superseded.INSTANCE, evidence, Optional.of(Require.text(reason, "reason")), now);
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
