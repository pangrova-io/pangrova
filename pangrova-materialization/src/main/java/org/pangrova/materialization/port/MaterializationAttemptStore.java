package org.pangrova.materialization.port;

import java.time.Instant;
import java.util.List;
import java.util.Optional;
import org.pangrova.materialization.domain.AttemptId;
import org.pangrova.materialization.domain.Generation;
import org.pangrova.materialization.domain.MaterializationId;
import org.pangrova.materialization.domain.MaterializationWriteCapability;
import org.pangrova.materialization.domain.RefreshAttempt;
import org.pangrova.materialization.domain.SourcePin;
import org.pangrova.materialization.domain.TenantId;
import org.pangrova.spi.materialization.CommitEvidence;

/**
 * Attempt reservation, commit evidence, publication, and source-pin authority
 * of the materialization kernel.
 *
 * <p>Attempt reservations use unique indexes on the active attempt and
 * candidate table. Recovery paths must settle or publish only under the state
 * the caller observed, so they can never overwrite a transition that won a
 * concurrent race.
 */
public interface MaterializationAttemptStore {

    /**
     * Lists refresh attempts, newest first.
     *
     * @param tenant tenant to inspect
     * @param object optional object filter
     * @param limit maximum number of attempts
     * @return attempts ordered by creation instant, newest first
     */
    List<RefreshAttempt> findAttempts(TenantId tenant, Optional<MaterializationId> object,
            int limit);

    /**
     * Finds an attempt by identity.
     *
     * @param attempt attempt identity
     * @return attempt when present
     */
    Optional<RefreshAttempt> findAttempt(AttemptId attempt);

    /**
     * Finds the one active (non-terminal) attempt of an object.
     *
     * @param object object identity
     * @return active attempt when present
     */
    Optional<RefreshAttempt> findActiveAttempt(MaterializationId object);

    /**
     * Finds the generation produced by an attempt.
     *
     * @param attempt attempt identity
     * @return generation when the attempt was reserved
     */
    Optional<Generation> findGenerationByAttempt(AttemptId attempt);

    /**
     * Lists attempts that may need reconciliation.
     *
     * <p>An attempt is reconcilable only when its execution anchor is no longer
     * live, or when it is quarantined and has been observed for the full grace
     * period. A live anchor means the owning coordinator may still be writing,
     * so recovery must never race it.
     *
     * @param observedBefore quarantine observation cutoff
     * @return attempts safe to reconcile
     */
    List<RefreshAttempt> findReconcilableAttempts(Instant observedBefore);

    /**
     * Marks an attempt as executing in the data plane.
     *
     * @param attempt attempt identity
     * @param now transition instant
     */
    void markAttemptBuilding(AttemptId attempt, Instant now);

    /**
     * Reserves an attempt, its candidate generation, source pins, and write
     * capability in one transaction.
     *
     * @param attempt attempt to reserve
     * @param candidate candidate generation
     * @param sourcePins acquired source pins
     * @param capability internal write capability
     * @param expectedObjectRevision head revision observed at admission
     * @param event reservation event
     */
    void reserveAttempt(RefreshAttempt attempt, Generation candidate, List<SourcePin> sourcePins,
            MaterializationWriteCapability capability, long expectedObjectRevision,
            MaterializationEvent event);

    /**
     * Finds the internal write capability issued for an attempt.
     *
     * @param attempt attempt identity
     * @return capability when the attempt was reserved
     */
    Optional<MaterializationWriteCapability> findWriteCapability(AttemptId attempt);

    /**
     * Records exact commit evidence and moves the attempt and its generation to committed.
     *
     * @param attemptId attempt identity
     * @param evidence exact connector evidence
     * @param now observation instant
     */
    void recordCommitEvidence(AttemptId attemptId, CommitEvidence evidence, Instant now);

    /**
     * Publishes the candidate: verifies fence and evidence, advances the head
     * with CAS, marks the generation published, retires the previous head,
     * settles the attempt, and records the command receipt.
     *
     * @param command publication command
     * @param event publication event
     * @return idempotent publication result
     */
    PublicationResult publish(PublishCommand command, MaterializationEvent event);

    /**
     * Settles an attempt and its candidate generation in a non-publishing state.
     *
     * <p>The update is conditional on the attempt still being in the state the
     * caller observed, so recovery can never overwrite a transition that won a
     * concurrent race.
     *
     * @param previous attempt state the caller observed
     * @param settled settled attempt
     * @param generation settled generation
     * @param event settlement event
     */
    void settleAttempt(RefreshAttempt previous, RefreshAttempt settled, Generation generation,
            MaterializationEvent event);

    /**
     * Lists acquired source pins that can be reclaimed without re-running data.
     *
     * <p>An attempt's pins are reclaimable when the attempt settled before
     * {@code settledBefore}, or when every hold expired at or before
     * {@code now}. The result contains all acquired pins of each selected
     * attempt so a durable release never hides a hold the sweep did not see.
     *
     * @param now current instant used to evaluate expiry
     * @param settledBefore only attempts settled before this instant qualify
     * @param attempts maximum number of distinct attempts to return
     * @return acquired pins of the selected attempts
     */
    List<ReclaimableSourcePin> findReclaimableSourcePins(
            Instant now, Instant settledBefore, int attempts);

    /**
     * Marks every acquired source pin of an attempt as released.
     *
     * <p>Called only after the physical holds were released. An attempt without
     * acquired pins is unchanged.
     *
     * @param attempt attempt whose pins were released
     * @param now release instant
     */
    void releaseSourcePins(AttemptId attempt, Instant now);
}
