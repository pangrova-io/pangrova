package org.pangrova.materialization.port;

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
 *
 * <p>The interface composes the object, attempt, and generation authorities
 * defined by {@link MaterializationObjectStore}, {@link MaterializationAttemptStore},
 * and {@link MaterializationGenerationStore}; callers that only need one
 * authority may depend on the narrower type.
 */
public interface MaterializationRepository extends MaterializationObjectStore,
        MaterializationAttemptStore, MaterializationGenerationStore {
}
