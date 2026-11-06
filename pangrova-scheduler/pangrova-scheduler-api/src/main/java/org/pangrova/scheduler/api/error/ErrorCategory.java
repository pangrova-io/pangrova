package org.pangrova.scheduler.api.error;

import org.pangrova.common.api.annotation.Internal;

/**
 * Classifies exceptions encountered during task dispatch to Workers.
 *
 * <p>Used by {@code RemoteTaskDispatcher#classifyAndWrap} to route exceptions
 * to appropriate handling paths. Network, RPC timeout and Worker fatal errors
 * count against the circuit breaker; backpressure and local control fencing do not.</p>
 */
@Internal
public enum ErrorCategory {

    /**
     * Network-level failure: connection refused, DNS resolution failure,
     * or general I/O error reaching the worker.
     */
    NETWORK,

    /**
     * Worker is alive but temporarily unable to accept tasks
     * (e.g., HTTP 503, task rejected due to resource pressure).
     */
    WORKER_BUSY,

    /** Local Coordinator epoch or execution ownership changed before dispatch. */
    CONTROL_FENCED,

    /**
     * Worker returned a fatal error indicating internal failure
     * (e.g., HTTP 500, unrecoverable processing error).
     * Circuit breaker counts this category.
     */
    WORKER_FATAL,

    /**
     * RPC call timed out waiting for a response from the worker.
     */
    RPC_TIMEOUT
}
