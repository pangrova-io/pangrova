package org.pangrova.materialization.domain;

import java.time.Instant;
import java.util.Optional;

/**
 * Durable header of a Dynamic Table or Materialized View.
 *
 * <p>The header is the single publication authority. It carries the CAS
 * revision, the current definition revision, the published generation, and the
 * lifecycle state. Every mutation requires the caller's expected revision;
 * losing the CAS raises a concurrent-publication failure.
 *
 * @param id object identity
 * @param kind object kind
 * @param incarnation current logical incarnation
 * @param definitionRevision current definition revision
 * @param revision head CAS revision
 * @param publishedGeneration currently published generation, when one exists
 * @param state lifecycle state
 * @param definer persisted definer principal
 * @param securityMode execution security mode
 * @param refreshMode configured refresh mode
 * @param incrementalStrategy configured incremental strategy; empty means AUTO
 * @param createdAt creation instant
 * @param updatedAt last mutation instant
 */
public record MaterializationObject(
        MaterializationId id,
        MaterializationKind kind,
        IncarnationId incarnation,
        long definitionRevision,
        long revision,
        Optional<GenerationId> publishedGeneration,
        ObjectState state,
        Principal definer,
        SecurityMode securityMode,
        RefreshMode refreshMode,
        Optional<StrategyId> incrementalStrategy,
        Instant createdAt,
        Instant updatedAt) {

    public MaterializationObject {
        if (id == null || kind == null || incarnation == null) {
            throw new IllegalArgumentException("id, kind, and incarnation must not be null");
        }
        if (definitionRevision < 1) {
            throw new IllegalArgumentException("definitionRevision must be positive");
        }
        if (revision < 0) {
            throw new IllegalArgumentException("revision must not be negative");
        }
        publishedGeneration = Require.notNull(publishedGeneration, "publishedGeneration");
        if (state == null || definer == null || securityMode == null || refreshMode == null) {
            throw new IllegalArgumentException("state, definer, securityMode, and refreshMode are required");
        }
        incrementalStrategy = Require.notNull(incrementalStrategy, "incrementalStrategy");
        if (refreshMode == RefreshMode.FULL && incrementalStrategy.isPresent()) {
            throw new IllegalArgumentException("FULL refresh mode cannot carry an incremental strategy");
        }
        if (state == ObjectState.Creating.INSTANCE && publishedGeneration.isPresent()) {
            throw new IllegalArgumentException("a creating object cannot have a published generation");
        }
        if (createdAt == null || updatedAt == null) {
            throw new IllegalArgumentException("createdAt and updatedAt must not be null");
        }
        if (updatedAt.isBefore(createdAt)) {
            throw new IllegalArgumentException("updatedAt must not precede createdAt");
        }
    }

    /**
     * Creates the initial header of an object whose first definition was validated.
     *
     * @param id object identity
     * @param kind object kind
     * @param incarnation new incarnation
     * @param definer persisted definer
     * @param securityMode execution security mode
     * @param refreshMode configured refresh mode
     * @param incrementalStrategy configured strategy; empty means AUTO
     * @param now creation instant
     * @return creating header
     */
    public static MaterializationObject creating(
            MaterializationId id,
            MaterializationKind kind,
            IncarnationId incarnation,
            Principal definer,
            SecurityMode securityMode,
            RefreshMode refreshMode,
            Optional<StrategyId> incrementalStrategy,
            Instant now) {
        return new MaterializationObject(id, kind, incarnation, 1, 0, Optional.empty(),
                ObjectState.Creating.INSTANCE, definer, securityMode, refreshMode,
                incrementalStrategy, now, now);
    }

    /**
     * Publishes a generation as the new head.
     *
     * @param generation generation becoming visible
     * @param expectedRevision head revision the caller observed
     * @param now mutation instant
     * @return updated header with an incremented revision
     */
    public MaterializationObject publish(GenerationId generation, long expectedRevision, Instant now) {
        requireRevision(expectedRevision);
        if (state != ObjectState.Creating.INSTANCE
                && state != ObjectState.Active.INSTANCE
                && state != ObjectState.Suspended.INSTANCE
                && state != ObjectState.Failed.INSTANCE) {
            throw MaterializationException.invalidStateTransition(
                    "cannot publish in object state " + state);
        }
        return new MaterializationObject(id, kind, incarnation, definitionRevision, revision + 1,
                Optional.of(Require.notNull(generation, "generation")), ObjectState.Active.INSTANCE,
                definer, securityMode, refreshMode, incrementalStrategy, createdAt, now);
    }

    /**
     * Transitions the header to a new lifecycle state.
     *
     * @param target desired state
     * @param expectedRevision head revision the caller observed
     * @param now mutation instant
     * @return updated header with an incremented revision
     */
    public MaterializationObject transitionTo(ObjectState target, long expectedRevision, Instant now) {
        requireRevision(expectedRevision);
        if (target == null || !state.canTransitionTo(target)) {
            throw MaterializationException.invalidStateTransition(
                    "object " + id.canonicalName() + " cannot transition from " + state + " to " + target);
        }
        return new MaterializationObject(id, kind, incarnation, definitionRevision, revision + 1,
                publishedGeneration, target, definer, securityMode, refreshMode,
                incrementalStrategy, createdAt, now);
    }

    /**
     * Replaces the definition and the refresh configuration in one header step.
     *
     * <p>{@code CREATE OR REPLACE} uses this so the next full refresh executes
     * the new definition under the new configuration. The published generation
     * is retained until that refresh publishes a replacement.
     *
     * @param newDefinitionRevision replacement definition revision
     * @param newRefreshMode replacement refresh mode
     * @param newStrategy replacement strategy; empty means AUTO
     * @param expectedRevision head revision the caller observed
     * @param now mutation instant
     * @return updated header with an incremented revision
     */
    public MaterializationObject withReplacement(
            long newDefinitionRevision,
            RefreshMode newRefreshMode,
            Optional<StrategyId> newStrategy,
            long expectedRevision,
            Instant now) {
        requireRevision(expectedRevision);
        if (newDefinitionRevision <= definitionRevision) {
            throw new IllegalArgumentException("replacement definition revision must advance");
        }
        if (newRefreshMode == null || newStrategy == null) {
            throw new IllegalArgumentException("refresh mode and strategy must not be null");
        }
        if (newRefreshMode == RefreshMode.FULL && newStrategy.isPresent()) {
            throw new IllegalArgumentException("FULL refresh mode cannot carry an incremental strategy");
        }
        return new MaterializationObject(id, kind, incarnation, newDefinitionRevision, revision + 1,
                publishedGeneration, state, definer, securityMode, newRefreshMode, newStrategy,
                createdAt, now);
    }

    /**
     * Moves this object to a new logical name in the same tenant.
     *
     * <p>The incarnation, definition revision, published generation, and
     * lifecycle state are preserved; the durable repository cascades the name
     * through definition, attempt, generation, dependency, and schedule rows.
     *
     * @param newId new object identity
     * @param expectedRevision head revision the caller observed
     * @param now mutation instant
     * @return renamed header with an incremented revision
     */
    public MaterializationObject renamed(MaterializationId newId, long expectedRevision, Instant now) {
        requireRevision(expectedRevision);
        if (newId == null) {
            throw new IllegalArgumentException("new identity must not be null");
        }
        if (!newId.tenant().equals(id.tenant())) {
            throw new IllegalArgumentException("a rename must stay within the same tenant");
        }
        if (newId.equals(id)) {
            throw new IllegalArgumentException("a rename must change the object name");
        }
        return new MaterializationObject(newId, kind, incarnation, definitionRevision, revision + 1,
                publishedGeneration, state, definer, securityMode, refreshMode, incrementalStrategy,
                createdAt, now);
    }

    /**
     * Records a new definition revision without changing the head generation.
     *
     * @param newDefinitionRevision new definition revision
     * @param expectedRevision head revision the caller observed
     * @param now mutation instant
     * @return updated header with an incremented revision
     */
    public MaterializationObject withDefinitionRevision(
            long newDefinitionRevision, long expectedRevision, Instant now) {
        requireRevision(expectedRevision);
        if (newDefinitionRevision <= definitionRevision) {
            throw new IllegalArgumentException("new definition revision must advance the current revision");
        }
        return new MaterializationObject(id, kind, incarnation, newDefinitionRevision, revision + 1,
                publishedGeneration, state, definer, securityMode, refreshMode,
                incrementalStrategy, createdAt, now);
    }

    /**
     * Changes refresh mode and incremental strategy.
     *
     * <p>Changing the mode requires a subsequent explicit full refresh before
     * the new mode takes effect; this method only records the configuration.
     *
     * @param newRefreshMode new refresh mode
     * @param newStrategy new strategy; empty means AUTO, forbidden for FULL
     * @param expectedRevision head revision the caller observed
     * @param now mutation instant
     * @return updated header with an incremented revision
     */
    public MaterializationObject withRefreshConfiguration(
            RefreshMode newRefreshMode,
            Optional<StrategyId> newStrategy,
            long expectedRevision,
            Instant now) {
        requireRevision(expectedRevision);
        if (newRefreshMode == null || newStrategy == null) {
            throw new IllegalArgumentException("refresh mode and strategy must not be null");
        }
        if (newRefreshMode == RefreshMode.FULL && newStrategy.isPresent()) {
            throw new IllegalArgumentException("FULL refresh mode cannot carry an incremental strategy");
        }
        return new MaterializationObject(id, kind, incarnation, definitionRevision, revision + 1,
                publishedGeneration, state, definer, securityMode, newRefreshMode,
                newStrategy, createdAt, now);
    }

    private void requireRevision(long expectedRevision) {
        if (revision != expectedRevision) {
            throw MaterializationException.concurrentPublication(
                    "expected head revision " + expectedRevision + " but found " + revision
                            + " for " + id.canonicalName());
        }
    }
}
