package org.pangrova.persistence.materialization.api.lifecycle;

import org.pangrova.materialization.api.enums.GcState;
import org.pangrova.common.api.annotation.Experimental;
import org.pangrova.materialization.api.port.CommandRecord;
import org.pangrova.materialization.api.port.GcHistoryEntry;
import org.pangrova.materialization.api.port.MaterializationEvent;
import org.pangrova.materialization.api.port.PublicationResult;
import org.pangrova.materialization.api.port.PublishCommand;
import org.pangrova.materialization.api.port.ReclaimableSourcePin;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import org.pangrova.materialization.api.domain.AttemptId;
import org.pangrova.materialization.api.domain.CommandId;
import org.pangrova.materialization.api.domain.DefinitionRevision;
import org.pangrova.materialization.api.domain.Dependency;
import org.pangrova.materialization.api.domain.GcLedgerEntry;
import org.pangrova.materialization.api.domain.Generation;
import org.pangrova.materialization.api.domain.GenerationId;
import org.pangrova.materialization.api.domain.MaterializationId;
import org.pangrova.materialization.api.domain.MaterializationObject;
import org.pangrova.materialization.api.domain.MaterializationWriteCapability;
import org.pangrova.materialization.api.domain.ReaderId;
import org.pangrova.materialization.api.domain.ReaderPin;
import org.pangrova.materialization.api.domain.RefreshAttempt;
import org.pangrova.materialization.api.domain.Schedule;
import org.pangrova.materialization.api.domain.SourcePin;
import org.pangrova.materialization.api.domain.TenantId;
import org.pangrova.connector.api.materialization.CommitEvidence;

/**
 * Durable authority of the materialization kernel.
 *
 * <p>Every mutating method is one database transaction. Object mutations use
 * CAS on the observed head revision; attempt reservations use unique indexes on
 * the active attempt and candidate table. Command ids are idempotency keys: a
 * replay must return the recorded result and must not repeat side effects.
 *
 * <p>The repository validates execution fences, commit evidence, source-vector
 * identity, and state transitions in SQL and triggers. It never performs
 * data-plane I/O and never processes row data.
 */
@Experimental
public interface MaterializationRepository {

    /**
     * Finds an object header.
     *
     * @param object object identity
     * @return header when present
     */
    Optional<MaterializationObject> findObject(MaterializationId object);

    /**
     * Finds an immutable definition revision.
     *
     * @param object object identity
     * @param revision definition revision
     * @return definition when present
     */
    Optional<DefinitionRevision> findDefinition(MaterializationId object, long revision);

    /**
     * Lists materializations whose current definition consumes the given object.
     *
     * <p>Only the latest recorded definition revision of each downstream object
     * counts, and objects already dropping or dropped are excluded. The result
     * is ordered by canonical name so callers can render deterministic errors.
     *
     * @param upstream object consumed by dependents
     * @return live dependent object identities
     */
    List<MaterializationId> findLiveDependents(MaterializationId upstream);

    /**
     * Lists every materialization object of a tenant.
     *
     * @param tenant tenant to list
     * @return objects ordered by canonical name, including dropped ones
     */
    List<MaterializationObject> listObjects(TenantId tenant);

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
     * Lists dependency edges.
     *
     * @param tenant tenant to inspect
     * @param object optional endpoint filter (downstream or upstream)
     * @return edges ordered by downstream, revision, and upstream
     */
    List<Dependency> findDependencies(
            TenantId tenant, Optional<MaterializationId> object);

    /**
     * Lists GC ledger entries joined with their owning object, newest first.
     *
     * @param tenant tenant to inspect
     * @param object optional object filter
     * @param limit maximum number of entries
     * @return GC history ordered by decision instant, newest first
     */
    List<GcHistoryEntry> findGcHistory(TenantId tenant, Optional<MaterializationId> object,
            int limit);

    /**
     * Finds a generation by identity.
     *
     * @param generation generation identity
     * @return generation when present
     */
    Optional<Generation> findGeneration(GenerationId generation);

    /**
     * Finds the currently published generation.
     *
     * @param object object identity
     * @return published generation when present
     */
    Optional<Generation> findPublishedGeneration(MaterializationId object);

    /**
     * Lists all retained generations of an object.
     *
     * @param object object identity
     * @return generations ordered by creation
     */
    List<Generation> findGenerations(MaterializationId object);

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
     * Finds the durable schedule of an object.
     *
     * @param object object identity
     * @return schedule when present
     */
    Optional<Schedule> findSchedule(MaterializationId object);

    /**
     * Finds a recorded command result within a tenant.
     *
     * @param tenant tenant that issued the command
     * @param commandId idempotency key
     * @return recorded result when the command already ran
     */
    Optional<CommandRecord> findCommand(TenantId tenant, CommandId commandId);

    /**
     * Creates an object header, its first definition, its schedule, and the
     * command receipt atomically.
     *
     * @param object creating header
     * @param definition first definition revision
     * @param schedule initial schedule
     * @param commandId idempotency key of the create command
     * @param event creation event
     */
    void createObject(MaterializationObject object, DefinitionRevision definition,
            Schedule schedule, CommandId commandId, MaterializationEvent event);

    /**
     * Creates an object only when its logical name is free.
     *
     * <p>Used by {@code IF NOT EXISTS}: losing the insert race is not an error
     * and must leave every table untouched.
     *
     * @param object creating header
     * @param definition first definition revision
     * @param schedule initial schedule
     * @param commandId idempotency key of the create command
     * @param event creation event
     * @return true when this call created the object
     */
    boolean createObjectIfAbsent(MaterializationObject object, DefinitionRevision definition,
            Schedule schedule, CommandId commandId, MaterializationEvent event);

    /**
     * Reclaims the logical name of a dropped object for a new incarnation.
     *
     * <p>The replacement keeps the durable audit trail (events, attempts, and
     * generations of the previous incarnation) but clears the definition and
     * schedule rows owned by the dropped incarnation, because those tables are
     * keyed by the logical name rather than the incarnation. The attempt fails
     * without side effects when the object is no longer dropped or has already
     * been reclaimed by a concurrent create.
     *
     * @param expectedDropped object header observed in the dropped state
     * @param replacement new incarnation header starting at revision 0
     * @param definition first definition revision of the new incarnation
     * @param schedule initial schedule of the new incarnation
     * @param commandId idempotency key of the create command
     * @param event creation event
     * @return true when this call reclaimed the name
     */
    boolean reclaimDroppedObject(MaterializationObject expectedDropped,
            MaterializationObject replacement, DefinitionRevision definition, Schedule schedule,
            CommandId commandId, MaterializationEvent event);

    /**
     * Renames an object and cascades its logical name through dependent rows.
     *
     * <p>The rename is a single transaction that CASes the header, rewrites the
     * name in definitions, generations, attempts, dependencies, and schedules,
     * records the command receipt, and appends the rename event. Historical
     * events and command receipts keep the name they were written under.
     *
     * @param object renamed header positioned at the new identity
     * @param previous previous object identity
     * @param expectedRevision head revision the caller observed
     * @param commandId idempotency key of the rename command
     * @param event rename event
     */
    void renameObject(MaterializationObject object, MaterializationId previous,
            long expectedRevision, CommandId commandId, MaterializationEvent event);

    /**
     * Records a new immutable definition revision and rebuilds its dependency edges.
     *
     * <p>The revision must be bound to the object's current incarnation and be
     * exactly one greater than the highest recorded revision. The object header
     * is not advanced; the caller follows with {@link #updateObject}.
     *
     * @param definition new definition revision
     */
    void recordDefinition(DefinitionRevision definition);

    /**
     * Records a new definition revision and advances the object header in one
     * transaction without changing the published generation.
     *
     * <p>The header must still be at {@code expectedRevision} and the definition
     * revision must be exactly one greater than the current head definition.
     *
     * @param definition new definition revision
     * @param object header advanced to the new definition revision
     * @param expectedRevision head revision the caller observed
     * @param commandId idempotency key of the alter command
     * @param event change event
     */
    void replaceDefinition(DefinitionRevision definition, MaterializationObject object,
            long expectedRevision, CommandId commandId, MaterializationEvent event);

    /**
     * Updates an object header with CAS.
     *
     * @param object desired header state
     * @param expectedRevision head revision the caller observed
     * @param commandId idempotency key
     * @param event change event
     */
    void updateObject(MaterializationObject object, long expectedRevision,
            CommandId commandId, MaterializationEvent event);

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
     * Completes a drop by clearing the head generation and retiring it.
     *
     * <p>The header CAS, generation retirement, active-attempt settlement, and
     * event append are one transaction so a crash cannot leave a dropped object
     * that still hides its physical table from garbage collection.
     *
     * @param dropped dropped header without a published generation
     * @param publishedGeneration generation referenced before the drop, when any
     * @param expectedRevision head revision the caller observed
     * @param commandId idempotency key
     * @param event drop event
     */
    void dropObject(MaterializationObject dropped, Optional<GenerationId> publishedGeneration,
            long expectedRevision, CommandId commandId, MaterializationEvent event);

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

    /**
     * Persists a generation transition such as retire, quarantine, or collect.
     *
     * @param generation desired generation state
     * @param event transition event
     */
    void saveGeneration(Generation generation, MaterializationEvent event);

    /**
     * Updates a schedule with CAS.
     *
     * @param schedule desired schedule state
     * @param expectedVersion schedule version the caller observed
     */
    void updateSchedule(Schedule schedule, long expectedVersion);

    /**
     * Atomically claims the next due schedule using a lease.
     *
     * @param owner scheduler owner
     * @param now claim instant
     * @param leaseDuration lease duration
     * @return claimed schedule when one is due
     */
    Optional<Schedule> claimDueSchedule(String owner, Instant now, Duration leaseDuration);

    /**
     * Records a command receipt.
     *
     * @param record command receipt
     */
    void recordCommand(CommandRecord record);

    /**
     * Records a reader pin on a generation.
     *
     * @param pin reader pin
     */
    void pinReader(ReaderPin pin);

    /**
     * Releases a reader pin.
     *
     * @param generation pinned generation
     * @param reader exact reader kind and identifier
     */
    void releaseReaderPin(GenerationId generation, ReaderId reader);

    /**
     * Releases every reader pin held by one query or refresh attempt.
     *
     * <p>Terminal query cleanup releases by typed reader identity because a reader may
     * have scanned several generations of the same or different objects.
     *
     * @param reader exact reader kind and identifier
     */
    void releaseReaderPins(ReaderId reader);

    /**
     * Renews every reader pin held by one query or refresh attempt.
     *
     * <p>Renewal must only extend the lease; an expired pin stays expired
     * unless its owner renews it before purge. Renewal cannot reinsert a purged or released pin.
     *
     * @param reader exact reader kind and identifier
     * @param expiresAt new expiry instant
     * @param heartbeatAt heartbeat instant
     */
    void renewReaderPins(ReaderId reader, Instant expiresAt, Instant heartbeatAt);

    /**
     * Deletes expired reader pins.
     *
     * @param now current instant
     * @return number of deleted pins
     */
    int purgeExpiredReaderPins(Instant now);

    /**
     * Lists reader pins of a generation.
     *
     * @param generation generation identity
     * @return recorded pins, including expired ones
     */
    List<ReaderPin> findReaderPins(GenerationId generation);

    /**
     * Lists generations whose physical tables are eligible for collection.
     *
     * <p>A generation is eligible when it is retired past its retention
     * deadline, or unpublished with a terminal attempt, and no live reader pin,
     * no active base reference, and no published head still depends on it.
     * Quarantined generations are never returned.
     *
     * @param now current instant used to evaluate reader pins
     * @param eligibleBefore only generations settled before this instant
     * @param limit maximum number of generations
     * @return eligible generations ordered by age
     */
    List<Generation> findCollectableGenerations(Instant now, Instant eligibleBefore, int limit);

    /**
     * Records a GC decision before any physical change.
     *
     * @param entry pending ledger entry
     */
    void recordGcDecision(GcLedgerEntry entry);

    /**
     * Collects one generation while holding its row lock.
     *
     * <p>The generation is re-validated under {@code FOR UPDATE} immediately
     * before the physical drop, so a reader pin or a new base reference that
     * appears after the candidate scan either blocks until collection finishes
     * or is rejected by the pin path. The physical drop runs inside the same
     * transaction; the durable state transition and the applied ledger entry
     * commit only after it succeeds.
     *
     * @param generation generation expected to be collectable
     * @param entry pending ledger entry
     * @param event collection event
     * @param physicalDrop physical table drop
     * @return true when the generation was collected
     */
    boolean collectGeneration(Generation generation, GcLedgerEntry entry,
            MaterializationEvent event, Runnable physicalDrop);

    /**
     * Marks a GC decision applied or failed.
     *
     * @param entryId ledger entry identity
     * @param state terminal decision state
     * @param now transition instant
     */
    void updateGcDecision(UUID entryId, GcState state, Instant now);
}
