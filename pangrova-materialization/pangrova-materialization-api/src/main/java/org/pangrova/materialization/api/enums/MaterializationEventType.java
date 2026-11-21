package org.pangrova.materialization.api.enums;

/**
 * Structured event types emitted by the materialization kernel.
 */
public enum MaterializationEventType {

    /** Object header and first definition were persisted. */
    OBJECT_CREATED,

    /** Object definition or configuration changed. */
    OBJECT_ALTERED,

    /** Object entered the dropping state. */
    OBJECT_DROPPED,

    /** A refresh attempt was durably reserved. */
    ATTEMPT_RESERVED,

    /** The data plane reported exact commit evidence. */
    NATIVE_COMMITTED,

    /** A generation became the published head. */
    GENERATION_PUBLISHED,

    /** An attempt failed before a durable commit. */
    ATTEMPT_FAILED,

    /** An attempt was quarantined because its commit outcome is ambiguous. */
    ATTEMPT_QUARANTINED,

    /** A generation was retired after a newer head was published. */
    GENERATION_RETIRED,

    /** Reconciliation adopted, superseded, or abandoned an attempt. */
    ATTEMPT_RECONCILED,

    /** Garbage collection marked a generation as collectable. */
    GC_MARKED,

    /** Garbage collection dropped a physical generation. */
    GC_COLLECTED,

    /** A transparent rewrite was applied. */
    REWRITE_APPLIED,

    /** A transparent rewrite was skipped by a guard. */
    REWRITE_SKIPPED,

    /** An internal write capability was issued. */
    CAPABILITY_ISSUED,

    /** Source policy fingerprints changed between planning and publication. */
    POLICY_DRIFT
}
