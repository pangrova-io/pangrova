package org.pangrova.observability.api.tracing;

import org.pangrova.common.api.annotation.Experimental;

/**
 * Immutable provider-owned execution correlation, independent of telemetry export.
 * Safe to pass between threads: it retains a snapshot, never an originating thread's
 * active scope. It carries only W3C trace identity/flags/state, not arbitrary baggage,
 * credentials, query authority or cancellation. Its originating provider owns activation.
 */
@Experimental
public interface ExecutionTraceContext {
    /** Valid nonzero 32-character lowercase hexadecimal W3C trace ID. */
    String traceId();
    /** Valid nonzero 16-character lowercase hexadecimal W3C span ID. */
    String spanId();
    /** Captured unsigned W3C flags byte, including provider-preserved bits. */
    int traceFlags();
    /** W3C-encoded vendor state, empty when absent. */
    String traceState();
    /**
     * Activates this snapshot on the calling thread without starting a recording span.
     * The caller closes the returned scope on that thread in nested LIFO order to
     * restore its previous context. Concurrent callers acquire independent scopes.
     * No query/resource ownership is transferred; closing does not end the parent span.
     */
    Scope makeCurrent();
}
