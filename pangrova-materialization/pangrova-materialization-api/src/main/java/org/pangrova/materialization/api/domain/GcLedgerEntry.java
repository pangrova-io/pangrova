package org.pangrova.materialization.api.domain;

import org.pangrova.materialization.api.enums.GcState;
import org.pangrova.materialization.api.enums.GcDecision;
import org.pangrova.materialization.api.exception.MaterializationException;

import java.util.UUID;

import static org.pangrova.common.util.Preconditions.requireText;

/**
 * Durable GC decision for one generation.
 *
 * <p>The entry is written before the physical table is dropped and is never
 * deleted, so every collection can be explained after the fact.
 *
 * @param entryId ledger entry identity
 * @param generation generation the decision applies to
 * @param decision GC decision
 * @param evidence human-readable decision evidence
 * @param state execution state of the decision
 * @param createdAt decision instant
 * @param updatedAt last state transition instant
 */
public record GcLedgerEntry(
        UUID entryId,
        GenerationId generation,
        GcDecision decision,
        String evidence,
        GcState state,
        java.time.Instant createdAt,
        java.time.Instant updatedAt) {

    public GcLedgerEntry {
        if (entryId == null || generation == null) {
            throw new IllegalArgumentException("entryId and generation must not be null");
        }
        if (decision == null || state == null) {
            throw new IllegalArgumentException("decision and state must not be null");
        }
        evidence = requireText(evidence, "evidence");
        if (createdAt == null || updatedAt == null) {
            throw new IllegalArgumentException("createdAt and updatedAt must not be null");
        }
        if (updatedAt.isBefore(createdAt)) {
            throw new IllegalArgumentException("updatedAt must not precede createdAt");
        }
    }

    /** Creates a pending collection decision. */
    public static GcLedgerEntry pending(GenerationId generation, String evidence,
            java.time.Instant now) {
        return new GcLedgerEntry(UUID.randomUUID(), generation, GcDecision.COLLECT,
                evidence, GcState.PENDING, now, now);
    }

    /**
     * Moves the decision to a terminal state.
     *
     * @param target target state
     * @param now transition instant
     * @return updated entry
     */
    public GcLedgerEntry transitionTo(GcState target, java.time.Instant now) {
        if (state != GcState.PENDING) {
            throw MaterializationException.invalidStateTransition(
                    "GC entry " + entryId + " is already " + state);
        }
        if (target == GcState.PENDING) {
            throw MaterializationException.invalidStateTransition(
                    "GC entry cannot transition back to pending");
        }
        return new GcLedgerEntry(entryId, generation, decision, evidence, target, createdAt, now);
    }

}
