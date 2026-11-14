package org.pangrova.observability.api.tracing;

import org.pangrova.common.api.annotation.Experimental;
import org.pangrova.observability.api.tracing.ExecutionTraceContext;

/**
 * Explicitly selected, thread-safe execution correlation provider.
 * Capture preserves the current valid parent and its flags/vendor state. Without a
 * valid parent it creates a distinct valid unsampled root; disabling export must not
 * erase execution identity. Capture does not activate a scope, start an exporter or
 * change thread context. Returned snapshots remain usable for the execution lifetime.
 * Selection belongs to process assembly, never a static mutable locator or session value.
 */
@Experimental
public interface ExecutionTraceContextFactory {
    /** Captures the current parent or a fresh unsampled root; never null or invalid. */
    ExecutionTraceContext capture();
}
