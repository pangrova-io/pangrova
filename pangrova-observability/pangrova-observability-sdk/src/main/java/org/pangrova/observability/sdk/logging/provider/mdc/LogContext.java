package org.pangrova.observability.sdk.logging.provider.mdc;

import java.util.Map;

import org.slf4j.MDC;

/**
 * MDC-based trace context management for structured logging.
 * Sets trace_id, span_id, and query_id into SLF4J MDC for log correlation.
 *
 * <p>Thread propagation: use {@link #snapshot()} to capture the current MDC state
 * and {@link #restore(Map)} to apply it on a different thread (e.g., via TaskDecorator).
 */
public final class LogContext {

    public static final String KEY_TRACE_ID = "trace_id";
    public static final String KEY_SPAN_ID = "span_id";
    public static final String KEY_QUERY_ID = "query_id";

    private LogContext() {}

    /**
     * Sets trace context into MDC for the current thread.
     */
    public static void set(String traceId, String spanId) {
        MDC.put(KEY_TRACE_ID, traceId);
        MDC.put(KEY_SPAN_ID, spanId);
    }

    /**
     * Sets query_id into MDC for the current thread.
     */
    public static void setQueryId(String queryId) {
        if (queryId != null) {
            MDC.put(KEY_QUERY_ID, queryId);
        }
    }

    /**
     * Clears all trace/query context from MDC for the current thread.
     */
    public static void clear() {
        MDC.remove(KEY_TRACE_ID);
        MDC.remove(KEY_SPAN_ID);
        MDC.remove(KEY_QUERY_ID);
    }

    /**
     * Captures the current MDC context map for propagation to another thread.
     *
     * @return immutable snapshot of current MDC, or null if empty
     */
    public static Map<String, String> snapshot() {
        return MDC.getCopyOfContextMap();
    }

    /**
     * Restores a previously captured MDC context map on the current thread.
     * Typically called at the start of an async task.
     *
     * @param contextMap the MDC snapshot to restore, may be null
     */
    public static void restore(Map<String, String> contextMap) {
        if (contextMap != null) {
            MDC.setContextMap(contextMap);
        } else {
            MDC.clear();
        }
    }
}
