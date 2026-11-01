package org.pangrova.testing.testkit;

import org.pangrova.observability.sdk.noop.NoOpLoggingProvider;
import org.pangrova.observability.sdk.noop.NoOpMetricsProvider;
import org.pangrova.observability.sdk.noop.NoOpTracingProvider;
import org.pangrova.observability.sdk.tracing.adapter.http.TracingHttpFilter;
import org.pangrova.spi.observability.LoggingProvider;
import org.pangrova.spi.observability.MetricsProvider;
import org.pangrova.spi.observability.TracingProvider;

/**
 * Centralized NoOp observability singletons for test code.
 * Eliminates repeated {@code new NoOpTracingProvider()} across 15+ test files.
 * All instances are thread-safe, stateless singletons.
 */
public final class TestObservability {

    /** Shared NoOp tracing provider — safe for concurrent test use. */
    public static final TracingProvider TRACING = new NoOpTracingProvider();

    /** Shared NoOp metrics provider — safe for concurrent test use. */
    public static final MetricsProvider METRICS = new NoOpMetricsProvider();

    /** Shared NoOp logging provider — safe for concurrent test use. */
    public static final LoggingProvider LOGGING = new NoOpLoggingProvider();

    /** Shared NoOp tracing HTTP filter — wraps {@link #TRACING}. */
    public static final TracingHttpFilter TRACING_HTTP_FILTER = new TracingHttpFilter(TRACING);

    private TestObservability() {
        // utility class
    }
}
