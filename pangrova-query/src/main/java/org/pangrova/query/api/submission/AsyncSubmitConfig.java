package org.pangrova.query.api.submission;

import org.pangrova.common.api.annotation.Internal;

/**
 * Configuration for asynchronous query submission behavior.
 *
 * <p>When {@code enabled} is true, execution proceeds in a background executor.
 * A submission without a durable acceptance receipt waits for a readable
 * endpoint, a dispatched resultless write, or terminal completion.
 *
 * <p>The {@code submitTimeoutMs} bounds an HTTP caller waiting for initial readiness.
 * Once durable acceptance has issued the receipt, execution is governed by the
 * query and task deadlines.
 *
 * @param enabled         whether async submit mode is active (default true)
 * @param submitTimeoutMs maximum time to wait for initial readiness when the
 *                        receipt still depends on it (default 30 000 ms)
 */
@Internal
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
