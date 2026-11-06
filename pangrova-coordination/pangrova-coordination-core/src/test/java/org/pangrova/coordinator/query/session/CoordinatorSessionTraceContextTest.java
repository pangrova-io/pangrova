package org.pangrova.coordinator.query.session;

import org.pangrova.query.api.session.QuerySession;

import org.junit.jupiter.api.Test;
import org.pangrova.execution.core.authority.StageExecutionContext;
import org.pangrova.observability.api.tracing.ExecutionTraceContext;
import org.pangrova.observability.sdk.tracing.nativebridge.NativeTraceExporter;
import org.pangrova.security.api.identity.UserIdentity;

import java.time.Instant;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

class CoordinatorSessionTraceContextTest {
    @Test
    void sessionPropertiesCannotReplaceItsExecutionTraceAndCopiesKeepItsExactSnapshot() throws Exception {
        var actor = new UserIdentity("actual-actor", Set.of(), Map.of(), Instant.now());
        var session = new QuerySession("before-admission",
                Map.of("trace_id", "spoofed", "span_id", "spoofed"), actor, "tenant", java.util.Optional.empty(), new org.pangrova.observability.sdk.tracing.otel.OtelExecutionTraceContextFactory().capture());
        var admitted = session.withQueryId("admitted-query");
        assertThat(admitted.traceContext()).isSameAs(session.traceContext());
        assertThat(admitted.traceContext().traceId()).isNotEqualTo("spoofed");
        var stage = new StageExecutionContext.Live(admitted);
        assertThat(stage.traceContext()).isSameAs(session.traceContext());
        var executor = Executors.newSingleThreadExecutor();
        try {
            executor.submit(() -> {
                try (var scope = stage.traceContext().makeCurrent()) {
                    assertThat(NativeTraceExporter.currentTraceId()).isEqualTo(session.traceContext().traceId());
                    assertThat(NativeTraceExporter.currentSpanId()).isEqualTo(session.traceContext().spanId());
                }
            }).get(5, TimeUnit.SECONDS);
        } finally {
            executor.shutdownNow();
            assertThat(executor.awaitTermination(5, TimeUnit.SECONDS)).isTrue();
        }
    }

    @Test
    void freezesActualCurrentQuerySpanRatherThanRecapturingTheDispatchThread() {
        var root = new org.pangrova.observability.sdk.tracing.otel.OtelExecutionTraceContextFactory().capture();
        try (var scope = root.makeCurrent()) {
            var session = new QuerySession("query", Map.of(), null, "tenant", java.util.Optional.empty(), new org.pangrova.observability.sdk.tracing.otel.OtelExecutionTraceContextFactory().capture());
            assertThat(session.traceContext().traceId()).isEqualTo(root.traceId());
            assertThat(session.withQueryId("query-copy").traceContext()).isSameAs(session.traceContext());
        }
    }
}
