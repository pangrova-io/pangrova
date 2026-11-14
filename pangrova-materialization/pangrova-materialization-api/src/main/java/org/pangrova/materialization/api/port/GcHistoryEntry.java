package org.pangrova.materialization.api.port;

import org.pangrova.materialization.api.enums.GcState;
import org.pangrova.materialization.api.enums.GcDecision;

import java.time.Instant;
import java.util.UUID;
import org.pangrova.materialization.api.domain.GenerationId;
import org.pangrova.materialization.api.domain.MaterializationId;

/**
 * Durable GC ledger entry joined with its owning materialization.
 *
 * @param object object whose generation was classified
 * @param entryId ledger entry identity
 * @param generation generation the decision applies to
 * @param decision GC decision
 * @param evidence human-readable decision evidence
 * @param state execution state of the decision
 * @param createdAt decision instant
 * @param updatedAt last state transition instant
 */
public record GcHistoryEntry(
        MaterializationId object,
        UUID entryId,
        GenerationId generation,
        GcDecision decision,
        String evidence,
        GcState state,
        Instant createdAt,
        Instant updatedAt) {

    public GcHistoryEntry {
        if (object == null || entryId == null || generation == null
                || decision == null || state == null || createdAt == null || updatedAt == null) {
            throw new IllegalArgumentException("GC history entry must be complete");
        }
        evidence = evidence == null ? "" : evidence;
    }
}
