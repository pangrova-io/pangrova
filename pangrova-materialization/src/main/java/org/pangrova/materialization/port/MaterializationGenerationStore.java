package org.pangrova.materialization.port;

import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import org.pangrova.materialization.domain.GcLedgerEntry;
import org.pangrova.materialization.domain.Generation;
import org.pangrova.materialization.domain.GenerationId;
import org.pangrova.materialization.domain.MaterializationId;
import org.pangrova.materialization.domain.QueryId;
import org.pangrova.materialization.domain.ReaderPin;
import org.pangrova.materialization.domain.TenantId;

/**
 * Generation lifecycle, GC ledger, and reader-pin authority of the
 * materialization kernel.
 *
 * <p>GC decisions are recorded durably before any physical change, and a
 * collected generation is re-validated under a row lock immediately before
 * the physical drop commits.
 */
public interface MaterializationGenerationStore {

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
     * Persists a generation transition such as retire, quarantine, or collect.
     *
     * @param generation desired generation state
     * @param event transition event
     */
    void saveGeneration(Generation generation, MaterializationEvent event);

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
     * @param query owning query
     */
    void releaseReaderPin(GenerationId generation, QueryId query);

    /**
     * Releases every reader pin held by a query.
     *
     * <p>Terminal query cleanup releases by query identity because a query may
     * have scanned several generations of the same or different objects.
     *
     * @param query owning query
     */
    void releaseReaderPins(QueryId query);

    /**
     * Renews every reader pin held by a query.
     *
     * <p>Renewal must only extend the lease; an expired pin stays expired
     * unless the owning query is still alive and calls back.
     *
     * @param query owning query
     * @param expiresAt new expiry instant
     * @param heartbeatAt heartbeat instant
     */
    void renewReaderPins(QueryId query, Instant expiresAt, Instant heartbeatAt);

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
    void updateGcDecision(UUID entryId, GcLedgerEntry.GcState state, Instant now);
}
