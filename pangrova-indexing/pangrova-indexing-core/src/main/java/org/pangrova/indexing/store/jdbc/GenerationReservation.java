package org.pangrova.indexing.store.jdbc;

import org.pangrova.indexing.api.state.IndexState;

import java.time.Instant;

/** Immutable arguments for reserving one fenced index generation. */
record GenerationReservation(
        String indexId,
        int newGeneration,
        String buildSnapshotId,
        Instant timestamp,
        Instant expectedUpdatedAt,
        IndexState expectedState,
        int expectedGeneration) {}
