package org.pangrova.shuffle.api.enums;

import org.pangrova.common.api.annotation.Experimental;

/** Independent guarantees; neither storage medium nor transport direction implies any of them. */
@Experimental(reason = "Provider session and exchange evidence bindings are being consolidated")
public enum ShuffleCapability {
    /** Reads become available after exchange publication. */
    BLOCKING_READ,
    /** A reader can consume while producers are still running, with explicit completion signalling. */
    PIPELINED_READ,
    /** A new consumer attempt can read the same selected, committed producer outputs. */
    CONSUMER_RETRY,
    /** A producer can retry without exposing partial or duplicate output from its previous attempt. */
    PRODUCER_RETRY,
    /** Only explicitly selected producer attempts can contribute to a published exchange. */
    SPECULATIVE_ATTEMPT_ISOLATION,
    /** Published data remains reachable after its compute Worker exits; storage failure is separate. */
    READ_AFTER_COMPUTE_WORKER_EXIT,
    /** Published data remains reachable after a storage Worker failure under the configured topology. */
    READ_AFTER_STORAGE_WORKER_FAILURE,
    /** A replacement Coordinator session can recover the same registration and publication evidence. */
    COORDINATOR_SESSION_RECOVERY,
    /** Reads address an individual logical partition. */
    PARTITION_READ,
    /** Reads address an explicit partition or producer range without selecting a different attempt. */
    RANGE_READ,
    /** Retention protects the published data for the advertised interval and active reader leases. */
    RETENTION,
    /** Cleanup is retryable and does not invalidate data still referenced by active readers. */
    CLEANUP
}
