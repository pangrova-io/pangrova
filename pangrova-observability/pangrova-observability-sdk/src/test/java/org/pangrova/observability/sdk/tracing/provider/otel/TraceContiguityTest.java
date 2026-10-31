package org.pangrova.observability.sdk.tracing.provider.otel;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import io.opentelemetry.api.trace.SpanId;
import io.opentelemetry.api.trace.TraceId;
import io.opentelemetry.sdk.testing.exporter.InMemorySpanExporter;
import io.opentelemetry.sdk.trace.SdkTracerProvider;
import io.opentelemetry.sdk.trace.data.SpanData;
import io.opentelemetry.sdk.trace.export.SimpleSpanProcessor;
import org.pangrova.observability.api.Context;
import org.pangrova.observability.api.Span;
import org.pangrova.observability.api.SpanKind;
import org.pangrova.observability.api.StatusCode;
import org.pangrova.observability.api.TextMapGetter;
import org.pangrova.observability.api.TextMapSetter;
import org.pangrova.observability.api.TracingProvider;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.pangrova.observability.sdk.tracing.adapter.http.TracingHttpFilter;
import org.pangrova.observability.sdk.tracing.QueryTracer;

/**
 * Verifies end-to-end trace contiguity across the distributed query path.
 *
 * <p>The production span chain is:
 * <ol>
 *   <li>Coordinator HTTP entry (TracingHttpFilter) — SERVER span, extracts W3C traceparent</li>
 *   <li>QueryTracer.startQuerySpan — INTERNAL child of the HTTP span</li>
 *   <li>QueryTracer.startPhaseSpan (parse, optimize, schedule) — INTERNAL children</li>
 *   <li>Coordinator injects trace context into outbound headers to worker</li>
 *   <li>Worker HTTP entry (WorkerTracingFilter) — SERVER span, extracts injected context</li>
 * </ol>
 *
 * <p>This test asserts that every child span:
 * <ul>
 *   <li>shares the same traceId as the root</li>
 *   <li>has a valid parentSpanId pointing to its actual parent</li>
 * </ul>
 *
 * <p>Verifies parent-child linkage end-to-end (F-CHAIN-OBS-07).
 */
class TraceContiguityTest {

    private InMemorySpanExporter spanExporter;
    private SdkTracerProvider tracerProvider;
    private OtelTracingProvider otelProvider;

    @BeforeEach
    void setUp() {
        spanExporter = InMemorySpanExporter.create();
        tracerProvider = SdkTracerProvider.builder()
                .addSpanProcessor(SimpleSpanProcessor.create(spanExporter))
                .build();
        otelProvider = new OtelTracingProvider(
                tracerProvider.get("pangrova-contiguity-test"));
    }

    @AfterEach
    void tearDown() {
        tracerProvider.close();
    }

    // ── Test: coordinator-side span chain ──

    @Test
    void coordinatorSpanChainSharesTraceIdAndParentLinks() {
        // 1. Coordinator HTTP entry — TracingHttpFilter creates a SERVER span
        //    (simulated: no actual HTTP headers, so the root span has no external parent)
        TracingHttpFilter httpFilter = new TracingHttpFilter(otelProvider);
        Map<String, String> emptyHeaders = new HashMap<>();
        Span httpSpan = httpFilter.startServerSpan(
                "POST /v1/statement", emptyHeaders, MAP_GETTER);

        // 2. QueryTracer creates a query span as child of the HTTP span
        QueryTracer queryTracer = new QueryTracer(otelProvider);
        Context httpContext = spanContext(httpSpan);
        Span querySpan = queryTracer.startQuerySpan("q-1", "SELECT 1", httpContext);

        // 3. Phase spans as children of the query span
        Context queryContext = spanContext(querySpan);
        Span parseSpan = queryTracer.startPhaseSpan("parse", queryContext);
        parseSpan.end();

        Span optimizeSpan = queryTracer.startPhaseSpan("optimize", queryContext);
        optimizeSpan.end();

        Span scheduleSpan = queryTracer.startPhaseSpan("schedule", queryContext);
        scheduleSpan.end();

        querySpan.end();
        httpFilter.endSpan(httpSpan, 200);

        // Assert the chain
        List<SpanData> spans = spanExporter.getFinishedSpanItems();
        assertThat(spans).hasSize(5);

        SpanData httpData = findSpan("POST /v1/statement");
        SpanData queryData = findSpan("query");
        SpanData parseData = findSpan("query.parse");
        SpanData optimizeData = findSpan("query.optimize");
        SpanData scheduleData = findSpan("query.schedule");

        String rootTraceId = httpData.getSpanContext().getTraceId();
        assertThat(rootTraceId).isNotEqualTo(TraceId.getInvalid());

        // All spans share the same traceId
        assertThat(queryData.getSpanContext().getTraceId())
                .as("query span must share root traceId")
                .isEqualTo(rootTraceId);
        assertThat(parseData.getSpanContext().getTraceId())
                .as("parse span must share root traceId")
                .isEqualTo(rootTraceId);
        assertThat(optimizeData.getSpanContext().getTraceId())
                .as("optimize span must share root traceId")
                .isEqualTo(rootTraceId);
        assertThat(scheduleData.getSpanContext().getTraceId())
                .as("schedule span must share root traceId")
                .isEqualTo(rootTraceId);

        // Parent linkage: query → http, phases → query
        assertThat(queryData.getParentSpanContext().getSpanId())
                .as("query span parent must be the HTTP span")
                .isEqualTo(httpData.getSpanContext().getSpanId());
        assertThat(parseData.getParentSpanContext().getSpanId())
                .as("parse span parent must be the query span")
                .isEqualTo(queryData.getSpanContext().getSpanId());
        assertThat(optimizeData.getParentSpanContext().getSpanId())
                .as("optimize span parent must be the query span")
                .isEqualTo(queryData.getSpanContext().getSpanId());
        assertThat(scheduleData.getParentSpanContext().getSpanId())
                .as("schedule span parent must be the query span")
                .isEqualTo(queryData.getSpanContext().getSpanId());
    }

    // ── Test: inject → extract preserves trace across coordinator → worker hop ──

    @Test
    void injectExtractPreservesTraceAcrossCoordinatorWorkerHop() {
        // Coordinator side: create a dispatch span
        Span coordinatorSpan = otelProvider.startSpan(
                "coordinator.dispatch", SpanKind.CLIENT);
        Context coordinatorCtx = spanContext(coordinatorSpan);

        // Coordinator injects trace context into outbound HTTP headers
        Map<String, String> wireHeaders = new HashMap<>();
        otelProvider.inject(coordinatorCtx, wireHeaders, Map::put);

        // Verify traceparent was injected
        assertThat(wireHeaders).containsKey("traceparent");

        // Worker side: extract context from received headers
        TracingHttpFilter workerFilter = new TracingHttpFilter(otelProvider);
        Span workerSpan = workerFilter.startServerSpan(
                "POST /v1/task", wireHeaders, MAP_GETTER);

        // Worker creates a task execution span
        Context workerCtx = spanContext(workerSpan);
        Span taskSpan = otelProvider.startSpan(
                "task.execute", SpanKind.INTERNAL, workerCtx);
        taskSpan.end();

        workerFilter.endSpan(workerSpan, 200);
        coordinatorSpan.end();

        // Assert the chain
        SpanData coordData = findSpan("coordinator.dispatch");
        SpanData workerData = findSpan("POST /v1/task");
        SpanData taskData = findSpan("task.execute");

        String rootTraceId = coordData.getSpanContext().getTraceId();
        assertThat(rootTraceId).isNotEqualTo(TraceId.getInvalid());

        // All three spans share the same traceId (trace crossed the wire)
        assertThat(workerData.getSpanContext().getTraceId())
                .as("worker span must share coordinator traceId after extract")
                .isEqualTo(rootTraceId);
        assertThat(taskData.getSpanContext().getTraceId())
                .as("task span must share coordinator traceId")
                .isEqualTo(rootTraceId);

        // Parent chain: worker → coordinator, task → worker
        assertThat(workerData.getParentSpanContext().getSpanId())
                .as("worker span parent must be the coordinator dispatch span")
                .isEqualTo(coordData.getSpanContext().getSpanId());
        assertThat(taskData.getParentSpanContext().getSpanId())
                .as("task span parent must be the worker span")
                .isEqualTo(workerData.getSpanContext().getSpanId());
    }

    // ── Test: broken chain detection — without explicit parent, spans detach ──

    @Test
    void withoutExplicitParentSpansDetachIntoDifferentTraces() {
        // Create two spans without linking them via parent context
        Span spanA = otelProvider.startSpan("spanA", SpanKind.SERVER);
        Span spanB = otelProvider.startSpan("spanB", SpanKind.INTERNAL);
        spanB.end();
        spanA.end();

        SpanData dataA = findSpan("spanA");
        SpanData dataB = findSpan("spanB");

        // They should NOT share a traceId (no activation or explicit parent)
        assertThat(dataB.getSpanContext().getTraceId())
                .as("without explicit parent, spans must be in different traces")
                .isNotEqualTo(dataA.getSpanContext().getTraceId());
        assertThat(dataB.getParentSpanContext().isValid())
                .as("detached span has no valid parent")
                .isFalse();
    }

    // ── Test: error status propagates without breaking the chain ──

    @Test
    void errorStatusDoesNotBreakSpanChain() {
        Span parent = otelProvider.startSpan("parent.op", SpanKind.SERVER);
        Context parentCtx = spanContext(parent);
        Span child = otelProvider.startSpan("child.op", SpanKind.INTERNAL, parentCtx);

        child.setStatus(StatusCode.ERROR, "task failed");
        child.recordException(new RuntimeException("OOM"));
        child.end();
        parent.end();

        SpanData parentData = findSpan("parent.op");
        SpanData childData = findSpan("child.op");

        // Chain is intact despite error
        assertThat(childData.getSpanContext().getTraceId())
                .isEqualTo(parentData.getSpanContext().getTraceId());
        assertThat(childData.getParentSpanContext().getSpanId())
                .isEqualTo(parentData.getSpanContext().getSpanId());

        // Error recorded correctly
        assertThat(childData.getStatus().getStatusCode())
                .isEqualTo(io.opentelemetry.api.trace.StatusCode.ERROR);
        assertThat(childData.getEvents()).isNotEmpty();
    }

    // ── Test: full distributed pipeline (coordinator HTTP → query → dispatch → worker) ──

    @Test
    void fullDistributedPipelineProducesContiguousChain() {
        // Step 1: Client sends request to coordinator — TracingHttpFilter extracts
        TracingHttpFilter coordFilter = new TracingHttpFilter(otelProvider);
        QueryTracer queryTracer = new QueryTracer(otelProvider);

        Map<String, String> clientHeaders = new HashMap<>();
        Span coordHttpSpan = coordFilter.startServerSpan(
                "POST /v1/statement", clientHeaders, MAP_GETTER);
        Context coordHttpCtx = spanContext(coordHttpSpan);

        // Step 2: QueryTracer creates query span
        Span querySpan = queryTracer.startQuerySpan("q-42", "SELECT * FROM t", coordHttpCtx);
        Context queryCtx = spanContext(querySpan);

        // Step 3: Parse phase
        Span parseSpan = queryTracer.startPhaseSpan("parse", queryCtx);
        parseSpan.end();

        // Step 4: Optimize phase
        Span optimizeSpan = queryTracer.startPhaseSpan("optimize", queryCtx);
        optimizeSpan.end();

        // Step 5: Coordinator creates a CLIENT span for the worker dispatch
        Span dispatchSpan = otelProvider.startSpan("worker.dispatch", SpanKind.CLIENT, queryCtx);
        Context dispatchCtx = spanContext(dispatchSpan);

        // Step 6: Inject trace context into wire (HTTP headers to worker)
        Map<String, String> workerHeaders = new HashMap<>();
        otelProvider.inject(dispatchCtx, workerHeaders, Map::put);

        // Step 7: Worker extracts and creates SERVER span
        TracingHttpFilter workerFilter = new TracingHttpFilter(otelProvider);
        Span workerHttpSpan = workerFilter.startServerSpan(
                "POST /v1/task", workerHeaders, MAP_GETTER);
        Context workerHttpCtx = spanContext(workerHttpSpan);

        // Step 8: Worker creates task execution span
        Span taskExecSpan = otelProvider.startSpan(
                "task.execute", SpanKind.INTERNAL, workerHttpCtx);
        taskExecSpan.end();

        // Cleanup
        workerFilter.endSpan(workerHttpSpan, 200);
        dispatchSpan.end();
        querySpan.end();
        coordFilter.endSpan(coordHttpSpan, 200);

        // Collect all spans
        List<SpanData> allSpans = spanExporter.getFinishedSpanItems();
        assertThat(allSpans).hasSize(7);

        SpanData coordHttp = findSpan("POST /v1/statement");
        SpanData query = findSpan("query");
        SpanData parse = findSpan("query.parse");
        SpanData optimize = findSpan("query.optimize");
        SpanData dispatch = findSpan("worker.dispatch");
        SpanData workerHttp = findSpan("POST /v1/task");
        SpanData taskExec = findSpan("task.execute");

        // ALL spans share one traceId
        String traceId = coordHttp.getSpanContext().getTraceId();
        assertThat(traceId).isNotEqualTo(TraceId.getInvalid());

        for (SpanData span : allSpans) {
            assertThat(span.getSpanContext().getTraceId())
                    .as("span '%s' must share root traceId", span.getName())
                    .isEqualTo(traceId);
        }

        // All span IDs are distinct
        List<String> spanIds = allSpans.stream()
                .map(s -> s.getSpanContext().getSpanId())
                .toList();
        assertThat(spanIds).doesNotHaveDuplicates();
        for (String sid : spanIds) {
            assertThat(sid).isNotEqualTo(SpanId.getInvalid());
        }

        // Parent chain:
        //   coordHttp (root) ← query ← { parse, optimize, dispatch }
        //   dispatch ← workerHttp ← taskExec
        assertThat(query.getParentSpanContext().getSpanId())
                .as("query → coordHttp")
                .isEqualTo(coordHttp.getSpanContext().getSpanId());
        assertThat(parse.getParentSpanContext().getSpanId())
                .as("parse → query")
                .isEqualTo(query.getSpanContext().getSpanId());
        assertThat(optimize.getParentSpanContext().getSpanId())
                .as("optimize → query")
                .isEqualTo(query.getSpanContext().getSpanId());
        assertThat(dispatch.getParentSpanContext().getSpanId())
                .as("dispatch → query")
                .isEqualTo(query.getSpanContext().getSpanId());
        assertThat(workerHttp.getParentSpanContext().getSpanId())
                .as("workerHttp → dispatch (across wire)")
                .isEqualTo(dispatch.getSpanContext().getSpanId());
        assertThat(taskExec.getParentSpanContext().getSpanId())
                .as("taskExec → workerHttp")
                .isEqualTo(workerHttp.getSpanContext().getSpanId());
    }

    // ── Helpers ──

    /**
     * Extracts an OtelContextWrapper from an OtelSpanWrapper so it can be
     * passed as explicit parent to subsequent startSpan calls.
     */
    private static Context spanContext(Span span) {
        io.opentelemetry.api.trace.Span otelSpan = ((OtelSpanWrapper) span).unwrap();
        io.opentelemetry.context.Context otelCtx =
                io.opentelemetry.context.Context.current().with(otelSpan);
        return new OtelContextWrapper(otelCtx);
    }

    private SpanData findSpan(String name) {
        List<SpanData> spans = spanExporter.getFinishedSpanItems();
        return spans.stream()
                .filter(s -> s.getName().equals(name))
                .findFirst()
                .orElseThrow(() -> new AssertionError(
                        "no span named '" + name + "' in " + spans));
    }

    private static final TextMapGetter<Map<String, String>> MAP_GETTER =
            new TextMapGetter<>() {
                @Override
                public Iterable<String> keys(Map<String, String> carrier) {
                    return carrier.keySet();
                }

                @Override
                public String get(Map<String, String> carrier, String key) {
                    return carrier.get(key);
                }
            };
}
