package org.pangrova.materialization.api.domain;

import org.pangrova.materialization.api.exception.MaterializationException;

import java.time.Instant;
import java.util.Optional;
import org.pangrova.connector.api.materialization.CommitEvidence;

import static org.pangrova.common.util.Preconditions.requireNonNullField;

/**
 * One physical generation of a materialized result.
 *
 * <p>A generation is created as a candidate before any data-plane work, gains
 * exact commit evidence when the connector commits, and becomes the published
 * head only through the object-header CAS. Incremental generations must name
 * their base generation; full generations must not.
 *
 * @param id generation identity
 * @param object owning object
 * @param incarnation object incarnation
 * @param definitionRevision definition revision that produced this generation
 * @param producingAttempt attempt that reserves and builds this generation
 * @param baseGeneration base generation for incremental kinds, empty for full refresh
 * @param physicalTable physical candidate table
 * @param refreshKind resolved refresh kind
 * @param sourceVector exact source snapshots
 * @param evidence exact commit evidence, present from the committed state onward
 * @param state generation state
 * @param createdAt creation instant
 * @param publishedAt instant at which the generation became the head
 */
public record Generation(
        GenerationId id,
        MaterializationId object,
        IncarnationId incarnation,
        long definitionRevision,
        AttemptId producingAttempt,
        Optional<GenerationId> baseGeneration,
        PhysicalTableRef physicalTable,
        RefreshKind refreshKind,
        SourceVector sourceVector,
        Optional<CommitEvidence> evidence,
        GenerationState state,
        Instant createdAt,
        Optional<Instant> publishedAt) {

    public Generation {
        requireIdentity(id, object, incarnation, producingAttempt);
        if (definitionRevision < 1) {
            throw new IllegalArgumentException("definitionRevision must be positive");
        }
        baseGeneration = requireNonNullField(baseGeneration, "baseGeneration");
        requirePayload(physicalTable, refreshKind, sourceVector, state);
        evidence = requireNonNullField(evidence, "evidence");
        publishedAt = requireNonNullField(publishedAt, "publishedAt");
        requireBaseConsistency(refreshKind, baseGeneration);
        requireEvidence(state, evidence);
        requirePublicationInstant(state, publishedAt);
        if (createdAt == null) {
            throw new IllegalArgumentException("createdAt must not be null");
        }
    }

    private static void requireIdentity(GenerationId id, MaterializationId object,
            IncarnationId incarnation, AttemptId producingAttempt) {
        if (id == null || object == null || incarnation == null || producingAttempt == null) {
            throw new IllegalArgumentException(
                    "id, object, incarnation, and producingAttempt must not be null");
        }
    }

    private static void requirePayload(PhysicalTableRef physicalTable, RefreshKind refreshKind,
            SourceVector sourceVector, GenerationState state) {
        if (physicalTable == null || refreshKind == null || sourceVector == null || state == null) {
            throw new IllegalArgumentException(
                    "physicalTable, refreshKind, sourceVector, and state are required");
        }
    }

    private static void requireBaseConsistency(RefreshKind refreshKind,
            Optional<GenerationId> baseGeneration) {
        if (refreshKind.isIncremental() && baseGeneration.isEmpty()) {
            throw new IllegalArgumentException("incremental generation must name a base generation");
        }
        if (!refreshKind.isIncremental() && baseGeneration.isPresent()) {
            throw new IllegalArgumentException("full generation must not name a base generation");
        }
    }

    private static void requireEvidence(GenerationState state,
            Optional<CommitEvidence> generationEvidence) {
        if ((state == GenerationState.Committed.INSTANCE
                || state == GenerationState.Published.INSTANCE)
                && generationEvidence.isEmpty()) {
            throw new IllegalArgumentException(
                    "committed or published generation requires commit evidence");
        }
    }

    private static void requirePublicationInstant(GenerationState state,
            Optional<Instant> publicationInstant) {
        if (state == GenerationState.Published.INSTANCE && publicationInstant.isEmpty()) {
            throw new IllegalArgumentException("published generation requires a publication instant");
        }
        if (state != GenerationState.Published.INSTANCE
                && state != GenerationState.Retired.INSTANCE
                && state != GenerationState.Collected.INSTANCE
                && publicationInstant.isPresent()) {
            throw new IllegalArgumentException(
                    "only published, retired, or collected generations carry a publication instant");
        }
    }

    /**
     * Creates a candidate generation bound to a reserved physical table.
     *
     * @param id generation identity
     * @param object owning object
     * @param incarnation object incarnation
     * @param definitionRevision definition revision
     * @param producingAttempt attempt reserving the generation
     * @param baseGeneration base generation for incremental kinds
     * @param physicalTable reserved physical table
     * @param refreshKind resolved refresh kind
     * @param sourceVector exact source snapshots
     * @param now creation instant
     * @return candidate generation
     */
    public static Generation candidate(
            GenerationId id,
            MaterializationId object,
            IncarnationId incarnation,
            long definitionRevision,
            AttemptId producingAttempt,
            Optional<GenerationId> baseGeneration,
            PhysicalTableRef physicalTable,
            RefreshKind refreshKind,
            SourceVector sourceVector,
            Instant now) {
        return new Generation(id, object, incarnation, definitionRevision, producingAttempt, baseGeneration,
                physicalTable, refreshKind, sourceVector, Optional.empty(),
                GenerationState.Candidate.INSTANCE, now, Optional.empty());
    }

    /**
     * Records exact commit evidence and moves the generation to committed.
     *
     * @param commitEvidence exact connector evidence
     * @param now transition instant
     * @return committed generation
     */
    public Generation commit(CommitEvidence commitEvidence, Instant now) {
        requireTransition(GenerationState.Committed.INSTANCE);
        if (evidence.isPresent()) {
            throw MaterializationException.evidenceMismatch("generation already has commit evidence");
        }
        return new Generation(id, object, incarnation, definitionRevision, producingAttempt, baseGeneration,
                physicalTable, refreshKind, sourceVector, Optional.of(requireNonNullField(commitEvidence, "evidence")),
                GenerationState.Committed.INSTANCE, createdAt, Optional.empty());
    }

    /**
     * Publishes the committed generation.
     *
     * @param now publication instant
     * @return published generation
     */
    public Generation publish(Instant now) {
        requireTransition(GenerationState.Published.INSTANCE);
        if (evidence.isEmpty()) {
            throw MaterializationException.evidenceMismatch(
                    "cannot publish generation " + id + " without commit evidence");
        }
        return new Generation(id, object, incarnation, definitionRevision, producingAttempt, baseGeneration,
                physicalTable, refreshKind, sourceVector, evidence,
                GenerationState.Published.INSTANCE, createdAt, Optional.of(requireNonNullField(now, "now")));
    }

    /**
     * Retires a previously published generation after a newer head was published.
     *
     * @return retired generation
     */
    public Generation retire() {
        requireTransition(GenerationState.Retired.INSTANCE);
        return withState(GenerationState.Retired.INSTANCE);
    }

    /**
     * Moves an ambiguous generation to quarantine; its physical table is retained.
     *
     * @return quarantined generation
     */
    public Generation quarantine() {
        requireTransition(GenerationState.Quarantined.INSTANCE);
        return withState(GenerationState.Quarantined.INSTANCE);
    }

    /**
     * Marks the generation collected after the physical table was dropped.
     *
     * @return collected generation
     */
    public Generation collect() {
        requireTransition(GenerationState.Collected.INSTANCE);
        return withState(GenerationState.Collected.INSTANCE);
    }

    private Generation withState(GenerationState target) {
        return new Generation(id, object, incarnation, definitionRevision, producingAttempt, baseGeneration,
                physicalTable, refreshKind, sourceVector, evidence, target, createdAt, publishedAt);
    }

    private void requireTransition(GenerationState target) {
        if (!state.canTransitionTo(target)) {
            throw MaterializationException.invalidStateTransition(
                    "generation " + id + " cannot transition from " + state + " to " + target);
        }
    }
}
