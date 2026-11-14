package org.pangrova.materialization.api.domain;

import org.pangrova.materialization.api.exception.MaterializationException;

import java.time.Instant;

/**
 * Narrow internal write authority for one materialization attempt.
 *
 * <p>Users never receive privileges on internal physical tables. Instead the
 * coordinator issues this capability inside the reservation transaction; it
 * authorizes writes to exactly one candidate table for one attempt, under one
 * definer, for a bounded time. Workers and native executors must validate the
 * candidate identity and expiry before opening the write.
 *
 * @param object owning object
 * @param incarnation object incarnation
 * @param definitionRevision definition revision
 * @param attempt owning attempt
 * @param candidateTable exact candidate table writes are allowed to
 * @param generation generation being built
 * @param sourceVectorHash canonical source-vector hash the attempt was planned with
 * @param definer definer whose authority the write represents
 * @param controlEpoch coordinator control epoch at issuance
 * @param issuedAt issuance instant
 * @param expiresAt instant after which the capability is void
 */
public record MaterializationWriteCapability(
        MaterializationId object,
        IncarnationId incarnation,
        long definitionRevision,
        AttemptId attempt,
        PhysicalTableRef candidateTable,
        GenerationId generation,
        String sourceVectorHash,
        Principal definer,
        long controlEpoch,
        Instant issuedAt,
        Instant expiresAt) {

    public MaterializationWriteCapability {
        if (object == null || incarnation == null || attempt == null
                || candidateTable == null || generation == null || definer == null) {
            throw new IllegalArgumentException("capability identity fields must not be null");
        }
        if (definitionRevision < 1) {
            throw new IllegalArgumentException("definitionRevision must be positive");
        }
        if (sourceVectorHash == null || !sourceVectorHash.matches("[0-9a-f]{64}")) {
            throw new IllegalArgumentException("sourceVectorHash must be a lowercase SHA-256 hex digest");
        }
        if (controlEpoch < 0) {
            throw new IllegalArgumentException("controlEpoch must not be negative");
        }
        if (issuedAt == null || expiresAt == null) {
            throw new IllegalArgumentException("issuedAt and expiresAt must not be null");
        }
        if (!expiresAt.isAfter(issuedAt)) {
            throw new IllegalArgumentException("expiresAt must be after issuedAt");
        }
    }

    /**
     * Returns whether the capability has expired.
     *
     * @param now current instant
     * @return true when expired
     */
    public boolean isExpiredAt(Instant now) {
        return !expiresAt.isAfter(now);
    }

    /**
     * Rejects an expired capability.
     *
     * @param now current instant
     * @throws MaterializationException when the capability is expired
     */
    public void requireValidAt(Instant now) {
        if (isExpiredAt(now)) {
            throw MaterializationException.writeCapabilityInvalid(
                    "write capability for attempt " + attempt + " expired at " + expiresAt);
        }
    }

    /**
     * Requires that the write targets the bound candidate table.
     *
     * @param table table a writer intends to modify
     * @throws MaterializationException when the table is not the bound candidate
     */
    public void requireCandidate(PhysicalTableRef table) {
        if (!candidateTable.sameTable(table)) {
            throw MaterializationException.writeCapabilityInvalid(
                    "write capability for attempt " + attempt + " is bound to "
                            + candidateTable.qualifiedName() + " but write targeted "
                            + (table == null ? "null" : table.qualifiedName()));
        }
    }
}
