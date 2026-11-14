package org.pangrova.observability.sdk.tracing.otel;

import org.pangrova.observability.api.tracing.ExecutionTraceContext;
import org.pangrova.observability.api.tracing.ExecutionTraceContextFactory;

/** Stateless OpenTelemetry correlation capture; no exporter or closeable resources are created. */
public final class OtelExecutionTraceContextFactory implements ExecutionTraceContextFactory {
    @Override
    public ExecutionTraceContext capture() {
        return OtelExecutionTraceContext.capture();
    }
}
