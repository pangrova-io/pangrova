package org.pangrova.coordinator.query.session;

import org.junit.jupiter.api.Test;
import org.pangrova.coordinator.query.execution.dispatch.dag.control.StageExecutionContext;
import org.pangrova.observability.tracing.ExecutionTraceContext;
import org.pangrova.observability.tracing.NativeTraceExporter;
import org.pangrova.spi.security.UserIdentity;

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
        var session = new CoordinatorSessionContext("before-admission",
                Map.of("trace_id", "spoofed", "span_id", "spoofed"), actor, "tenant");
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
        var root = ExecutionTraceContext.capture();
        try (var scope = root.makeCurrent()) {
            var session = new CoordinatorSessionContext("query", Map.of(), null, "tenant");
            assertThat(session.traceContext().traceId()).isEqualTo(root.traceId());
            assertThat(session.withQueryId("query-copy").traceContext()).isSameAs(session.traceContext());
        }
    }
}
