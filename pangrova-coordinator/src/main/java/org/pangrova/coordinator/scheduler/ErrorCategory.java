package org.pangrova.coordinator.scheduler;

/**
 * Classifies exceptions encountered during task dispatch to Workers.
 *
 * <p>Used by {@link RemoteTaskDispatcher#classifyAndWrap} to route exceptions
 * to appropriate handling paths. Only {@link #WORKER_FATAL} triggers circuit
 * breaker counting — transient network issues and busy workers should not
 * open the circuit.</p>
 */
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
