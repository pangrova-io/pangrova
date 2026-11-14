package org.pangrova.materialization.usecase;

import org.pangrova.materialization.api.enums.GcState;

import java.time.Instant;
import java.util.List;
import java.util.Optional;
import org.pangrova.materialization.api.domain.GcLedgerEntry;
import org.pangrova.materialization.api.domain.Generation;
import org.pangrova.materialization.api.enums.GenerationState;
import org.pangrova.materialization.api.domain.Principal;
import org.pangrova.materialization.api.port.Clock;
import org.pangrova.materialization.api.port.MaterializationEvent;
import org.pangrova.materialization.api.enums.MaterializationEventType;
import org.pangrova.persistence.materialization.api.lifecycle.MaterializationRepository;
import org.pangrova.materialization.api.port.PhysicalGenerationStore;

import java.util.UUID;

/**
 * Two-phase physical garbage collector.
 *
 * <p>Each decision is written to the GC ledger before the physical table is
 * dropped and is only marked applied after the durable generation state is
 * collected. A crash between the phases leaves a pending entry whose retry is
 * safe because both the drop and the state transition are idempotent.
 * Quarantined generations and generations protected by a live reader pin or an
 * active base reference are never listed by the repository.
 */
public final class GenerationGarbageCollector {

    private static final Principal SYSTEM = new Principal("system:materialization-gc");

    private final MaterializationRepository repository;
    private final PhysicalGenerationStore store;
    private final Clock clock;

    /**
     * Creates a collector over the durable ledger and the physical store.
     *
     * @param repository durable repository
     * @param store physical generation store
     * @param clock time source
     */
    public GenerationGarbageCollector(MaterializationRepository repository,
            PhysicalGenerationStore store, Clock clock) {
        if (repository == null || store == null || clock == null) {
            throw new IllegalArgumentException("GC collaborators must not be null");
        }
        this.repository = repository;
        this.store = store;
        this.clock = clock;
    }

    /**
     * Collects one batch of eligible generations.
     *
     * @param policy retention and batch policy
     * @return collection counters
     */
    public GcReport runOnce(GcPolicy policy) {
        if (policy == null) {
            throw new IllegalArgumentException("policy must not be null");
        }
        Instant now = clock.now();
        repository.purgeExpiredReaderPins(now);
        List<Generation> candidates = repository.findCollectableGenerations(
                now, now.minus(policy.retention()), policy.batchSize());
        int collected = 0;
        int skipped = 0;
        int failed = 0;
        for (Generation generation : candidates) {
            if (!generation.state().canTransitionTo(GenerationState.COLLECTED)) {
                skipped++;
                continue;
            }
            GcLedgerEntry entry = GcLedgerEntry.pending(generation.id(),
                    "state=" + stateName(generation.state())
                            + " attempt=" + generation.producingAttempt().value()
                            + " table=" + generation.physicalTable().qualifiedName(),
                    now);
            repository.recordGcDecision(entry);
            try {
                boolean collectedNow = repository.collectGeneration(generation, entry,
                        event(generation, now),
                        () -> store.drop(generation.object(), generation.physicalTable()));
                if (collectedNow) {
                    collected++;
                } else {
                    skipped++;
                }
            } catch (RuntimeException failure) {
                markFailed(entry, failure);
                failed++;
            }
        }
        return new GcReport(candidates.size(), collected, skipped, failed);
    }

    private void markFailed(GcLedgerEntry entry, RuntimeException failure) {
        try {
            repository.updateGcDecision(
                    entry.entryId(), GcState.FAILED, clock.now());
        } catch (RuntimeException ledgerFailure) {
            failure.addSuppressed(ledgerFailure);
        }
    }

    private static MaterializationEvent event(Generation generation, Instant now) {
        return new MaterializationEvent(UUID.randomUUID().toString(),
                MaterializationEventType.GC_COLLECTED, generation.object(), Optional.empty(),
                Optional.of(generation.producingAttempt()), Optional.of(generation.id()),
                Optional.empty(), SYSTEM, SYSTEM,
                "collected " + generation.physicalTable().qualifiedName(), now);
    }

    private static String stateName(GenerationState state) {
        if (state == GenerationState.CANDIDATE) {
            return "CANDIDATE";
        }
        if (state == GenerationState.COMMITTED) {
            return "COMMITTED";
        }
        if (state == GenerationState.PUBLISHED) {
            return "PUBLISHED";
        }
        if (state == GenerationState.RETIRED) {
            return "RETIRED";
        }
        if (state == GenerationState.QUARANTINED) {
            return "QUARANTINED";
        }
        return "COLLECTED";
    }
}
