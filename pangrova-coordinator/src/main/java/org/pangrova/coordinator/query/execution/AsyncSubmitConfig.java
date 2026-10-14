package org.pangrova.coordinator.query.execution;

import org.pangrova.coordinator.query.QueryManager;

/**
 * Configuration for asynchronous query submission behavior.
 *
 * <p>When {@code enabled} is true, {@link QueryManager#submitQuery} returns
 * as soon as the query reaches RUNNING state (OUTPUT stage dispatched,
 * resultEndpoint known), rather than blocking until the entire DAG completes.
 * Execution continues in a background executor detached from the request thread.
 *
 * <p>The {@code submitTimeoutMs} bounds only the compilation + initial dispatch
 * phase. It is deliberately decoupled from the task execution timeout, which
 * governs how long individual tasks may run on workers.
 *
 * @param enabled         whether async submit mode is active (default true)
 * @param submitTimeoutMs maximum time to wait for the query to reach RUNNING
 *                        state before returning a timeout error (default 30 000 ms)
 */
public record AsyncSubmitConfig(boolean enabled, long submitTimeoutMs) {

    /** Default submit timeout: 30 seconds covers compilation + dispatch. */
    public static final long DEFAULT_SUBMIT_TIMEOUT_MS = 30_000;

    public AsyncSubmitConfig {
        if (submitTimeoutMs <= 0) {
            throw new IllegalArgumentException(
                    "submitTimeoutMs must be positive, got: " + submitTimeoutMs);
        }
    }

    /** Async submission enabled with default 30s submit timeout. */
    public static AsyncSubmitConfig defaultEnabled() {
        return new AsyncSubmitConfig(true, DEFAULT_SUBMIT_TIMEOUT_MS);
    }

    /** Synchronous mode — submit blocks until execution completes (legacy behavior). */
    public static AsyncSubmitConfig disabled() {
        return new AsyncSubmitConfig(false, DEFAULT_SUBMIT_TIMEOUT_MS);
    }
}
