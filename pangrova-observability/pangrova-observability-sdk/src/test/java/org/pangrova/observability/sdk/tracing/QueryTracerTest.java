package org.pangrova.observability.sdk.tracing;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.pangrova.spi.observability.Context;
import org.pangrova.spi.observability.Span;
import org.pangrova.spi.observability.SpanKind;
import org.pangrova.spi.observability.StatusCode;
import org.pangrova.spi.observability.TextMapGetter;
import org.pangrova.spi.observability.TextMapSetter;
import org.pangrova.spi.observability.TracingProvider;
import org.junit.jupiter.api.Test;

/**
 * Verifies {@link QueryTracer} span creation and attribute wiring,
 * including the parent-context overload added for T-0.5.1.
 */
class QueryTracerTest {

    @Test
    void startQuerySpan_withoutParent_createsRootSpan() {
        RecordingTracingProvider provider = new RecordingTracingProvider();
        QueryTracer tracer = new QueryTracer(provider);

        Span span = tracer.startQuerySpan("q-001", "SELECT 1");

        assertThat(provider.spans).hasSize(1);
        RecordingSpan recorded = provider.spans.get(0);
        assertThat(recorded.name).isEqualTo("query");
        assertThat(recorded.kind).isEqualTo(SpanKind.INTERNAL);
        assertThat(recorded.parent).isNull();
        assertThat(recorded.attributes).containsEntry("pangrova.query.id", "q-001");
        assertThat(recorded.attributes).containsEntry("pangrova.query.sql", "SELECT 1");
    }

    @Test
    void startQuerySpan_withParent_linksToParentContext() {
        RecordingTracingProvider provider = new RecordingTracingProvider();
        QueryTracer tracer = new QueryTracer(provider);
        Context parentCtx = new StubContext("parent-trace");

        Span span = tracer.startQuerySpan("q-002", "SELECT 2", parentCtx);

        assertThat(provider.spans).hasSize(1);
        RecordingSpan recorded = provider.spans.get(0);
        assertThat(recorded.name).isEqualTo("query");
        assertThat(recorded.kind).isEqualTo(SpanKind.INTERNAL);
        assertThat(recorded.parent).isSameAs(parentCtx);
        assertThat(recorded.attributes).containsEntry("pangrova.query.id", "q-002");
    }

    @Test
    void startPhaseSpan_usesParentContext() {
        RecordingTracingProvider provider = new RecordingTracingProvider();
        QueryTracer tracer = new QueryTracer(provider);
        Context parentCtx = new StubContext("phase-parent");

        Span span = tracer.startPhaseSpan("optimize", parentCtx);

        assertThat(provider.spans).hasSize(1);
        RecordingSpan recorded = provider.spans.get(0);
        assertThat(recorded.name).isEqualTo("query.optimize");
        assertThat(recorded.parent).isSameAs(parentCtx);
    }

    @Test
    void truncateSql_longSqlIsTruncated() {
        String longSql = "X".repeat(QueryTracer.MAX_SQL_LENGTH + 100);
        String result = QueryTracer.truncateSql(longSql);
        assertThat(result).hasSize(QueryTracer.MAX_SQL_LENGTH + 3);
        assertThat(result).endsWith("...");
    }

    @Test
    void truncateSql_nullReturnsEmpty() {
        assertThat(QueryTracer.truncateSql(null)).isEmpty();
    }

    @Test
    void truncateSql_shortSqlUnchanged() {
        assertThat(QueryTracer.truncateSql("SELECT 1")).isEqualTo("SELECT 1");
    }

    // --- test doubles (no mock frameworks) ---

    private static final class StubContext implements Context {
        final String label;
        StubContext(String label) { this.label = label; }
    }

    private static final class RecordingTracingProvider implements TracingProvider {
        final List<RecordingSpan> spans = new ArrayList<>();

        @Override
        public Span startSpan(String name, SpanKind kind) {
            RecordingSpan s = new RecordingSpan(name, kind, null);
            spans.add(s);
            return s;
        }

        @Override
        public Span startSpan(String name, SpanKind kind, Context parent) {
            RecordingSpan s = new RecordingSpan(name, kind, parent);
            spans.add(s);
            return s;
        }

        @Override
        public Span currentSpan() { return new RecordingSpan("current", SpanKind.INTERNAL, null); }

        @Override
        public <C> void inject(Context context, C carrier, TextMapSetter<C> setter) { }

        @Override
        public <C> Context extract(Context context, C carrier, TextMapGetter<C> getter) {
            return context;
        }
    }

    private static final class RecordingSpan implements Span {
        final String name;
        final SpanKind kind;
        final Context parent;
        final Map<String, String> attributes = new HashMap<>();

        RecordingSpan(String name, SpanKind kind, Context parent) {
            this.name = name;
            this.kind = kind;
            this.parent = parent;
        }

        @Override public Span setAttribute(String key, String value) {
            attributes.put(key, value);
            return this;
        }
        @Override public Span setAttribute(String key, long value) {
            attributes.put(key, String.valueOf(value));
            return this;
        }
        @Override public Span setStatus(StatusCode statusCode, String description) { return this; }
        @Override public Span recordException(Throwable exception) { return this; }
        @Override public void end() { }
    }
}
