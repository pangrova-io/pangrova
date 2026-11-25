package org.pangrova.persistence.indexing.postgresql.metadata;

import org.pangrova.indexing.api.enums.IndexState;

import java.time.Instant;

/** Immutable arguments for reserving one fenced index generation. */
record GenerationReservation(
        String indexId,
        int newGeneration,
        String buildSnapshotId,
        Instant timestamp,
        Instant expectedUpdatedAt,
        IndexState expectedState,
        int expectedGeneration,
        String sourceSchemaFingerprint) {}
