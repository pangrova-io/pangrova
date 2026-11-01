package org.pangrova.materialization.port;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import org.pangrova.materialization.domain.CommandId;
import org.pangrova.materialization.domain.DefinitionRevision;
import org.pangrova.materialization.domain.Dependency;
import org.pangrova.materialization.domain.GenerationId;
import org.pangrova.materialization.domain.MaterializationId;
import org.pangrova.materialization.domain.MaterializationObject;
import org.pangrova.materialization.domain.Schedule;
import org.pangrova.materialization.domain.TenantId;

/**
 * Object, definition, schedule, and command-receipt authority of the
 * materialization kernel.
 *
 * <p>Object mutations use CAS on the observed head revision; schedule
 * mutations use CAS on the schedule version. Command ids are idempotency
 * keys: a replay must return the recorded result and must not repeat side
 * effects.
 */
public interface MaterializationObjectStore {

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
     * Lists dependency edges.
     *
     * @param tenant tenant to inspect
     * @param object optional endpoint filter (downstream or upstream)
     * @return edges ordered by downstream, revision, and upstream
     */
    List<Dependency> findDependencies(
            TenantId tenant, Optional<MaterializationId> object);

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
}
