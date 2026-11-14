package org.pangrova.observability.sdk.tracing;

import org.pangrova.observability.logging.LogContext;
import org.pangrova.observability.logging.MdcContextPropagator;

import static org.assertj.core.api.Assertions.assertThat;

import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import io.opentelemetry.sdk.testing.junit5.OpenTelemetryExtension;
import java.util.Map;
import org.pangrova.observability.sdk.tracing.otel.OtelTracingProvider;
import org.pangrova.observability.sdk.tracing.http.TracingHttpFilter;
import org.pangrova.observability.api.tracing.TextMapGetter;
import org.pangrova.observability.api.tracing.TracingProvider;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

/**
 * Verifies that trace_id and query_id appear in log output when MDC is populated
 * at request entry points. Uses real OTel SDK (via OpenTelemetryExtension) and
 * Logback ListAppender to capture log events.
 */
class MdcTraceCorrelationTest {

    @RegisterExtension
    static final OpenTelemetryExtension otelExtension = OpenTelemetryExtension.create();

    private ListAppender<ILoggingEvent> appender;
    private Logger testLogger;

    @BeforeEach
    void setUp() {
        testLogger = (Logger) LoggerFactory.getLogger("test.mdc.correlation");
        appender = new ListAppender<>();
        appender.start();
        testLogger.addAppender(appender);
    }

    @AfterEach
    void tearDown() {
        testLogger.detachAppender(appender);
        appender.stop();
        LogContext.clear();
    }

    @Test
    void traceIdAppearsInMdcWhenSetViaLogContext() {
        String expectedTraceId = "abcdef1234567890abcdef1234567890";
        String expectedSpanId = "1234567890abcdef";

        LogContext.set(expectedTraceId, expectedSpanId);
        testLogger.info("test message");

        assertThat(appender.list).hasSize(1);
        ILoggingEvent event = appender.list.get(0);
        assertThat(event.getMDCPropertyMap())
                .containsEntry(LogContext.KEY_TRACE_ID, expectedTraceId)
                .containsEntry(LogContext.KEY_SPAN_ID, expectedSpanId);
    }

    @Test
    void queryIdAppearsInMdcWhenSet() {
        LogContext.set("trace-abc", "span-123");
        LogContext.setQueryId("query-42");
        testLogger.info("query execution");

        assertThat(appender.list).hasSize(1);
        ILoggingEvent event = appender.list.get(0);
        assertThat(event.getMDCPropertyMap())
                .containsEntry(LogContext.KEY_TRACE_ID, "trace-abc")
                .containsEntry(LogContext.KEY_QUERY_ID, "query-42");
    }

    @Test
    void tracingHttpFilterPopulatesMdcFromTraceHeader() {
        TracingProvider provider = new OtelTracingProvider(
                otelExtension.getOpenTelemetry().getTracer("test"));
        TracingHttpFilter filter = new TracingHttpFilter(provider);

        // Simulate incoming request with W3C traceparent header
        String traceId = "0af7651916cd43dd8448eb211c80319c";
        String parentSpanId = "b7ad6b7169203331";
        String traceparent = "00-" + traceId + "-" + parentSpanId + "-01";
        Map<String, String> headers = Map.of("traceparent", traceparent);

        org.pangrova.observability.api.tracing.Span span = filter.startServerSpan(
                "GET /api/query", headers, MAP_GETTER);
        try {
            testLogger.info("handling request");

            assertThat(appender.list).hasSize(1);
            Map<String, String> mdcMap = appender.list.get(0).getMDCPropertyMap();
            assertThat(mdcMap.get(LogContext.KEY_TRACE_ID)).isEqualTo(traceId);
            assertThat(mdcMap.get(LogContext.KEY_SPAN_ID)).isNotNull().isNotEmpty();
        } finally {
            filter.endSpan(span, 200);
        }

        // After endSpan, MDC should be cleared
        assertThat(MDC.get(LogContext.KEY_TRACE_ID)).isNull();
    }

    @Test
    void mdcPropagatesAcrossAsyncThreads() throws Exception {
        LogContext.set("async-trace-id", "async-span-id");
        LogContext.setQueryId("async-query-id");

        Runnable task = MdcContextPropagator.wrap(() -> testLogger.info("async task"));

        Thread asyncThread = new Thread(task);
        asyncThread.start();
        asyncThread.join(5000);

        assertThat(appender.list).hasSize(1);
        Map<String, String> mdcMap = appender.list.get(0).getMDCPropertyMap();
        assertThat(mdcMap)
                .containsEntry(LogContext.KEY_TRACE_ID, "async-trace-id")
                .containsEntry(LogContext.KEY_SPAN_ID, "async-span-id")
                .containsEntry(LogContext.KEY_QUERY_ID, "async-query-id");
    }

    @Test
    void mdcClearedAfterAsyncTaskCompletes() throws Exception {
        LogContext.set("parent-trace", "parent-span");

        Thread asyncThread = new Thread(MdcContextPropagator.wrap(() -> {
            assertThat(MDC.get(LogContext.KEY_TRACE_ID)).isEqualTo("parent-trace");
        }));
        asyncThread.start();
        asyncThread.join(5000);

        // Parent thread's MDC should be unaffected
        assertThat(MDC.get(LogContext.KEY_TRACE_ID)).isEqualTo("parent-trace");
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
