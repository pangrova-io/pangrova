package org.pangrova.worker.http.endpoint;

import static org.assertj.core.api.Assertions.assertThat;

import java.net.URI;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import io.opentelemetry.sdk.testing.exporter.InMemorySpanExporter;
import io.opentelemetry.sdk.trace.SdkTracerProvider;
import io.opentelemetry.sdk.trace.data.SpanData;
import io.opentelemetry.sdk.trace.export.SimpleSpanProcessor;
import jakarta.ws.rs.container.ContainerRequestContext;
import jakarta.ws.rs.container.ContainerResponseContext;
import jakarta.ws.rs.core.MultivaluedHashMap;
import jakarta.ws.rs.core.MultivaluedMap;
import jakarta.ws.rs.core.UriInfo;
import org.pangrova.observability.sdk.tracing.otel.OtelTracingProvider;
import org.pangrova.observability.sdk.tracing.http.TracingHttpFilter;
import org.pangrova.observability.api.tracing.Span;
import org.pangrova.observability.api.enums.SpanKind;
import org.pangrova.observability.api.tracing.TracingProvider;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Verifies {@link WorkerTracingFilter} closes the coordinator→worker trace gap (F-CHAIN-OBS-02):
 * the inbound traceparent header must be extracted so the worker SERVER span parents to the
 * coordinator-injected trace, and the span must be activated so task-execution work the resource
 * method performs (its own {@code task.submit} span, native traceId, MDC) attaches to the same trace.
 */
class WorkerTracingFilterTest {

    // traceparent the coordinator injects: trace 4bf9…4736, parent span 00f067aa0ba902b7.
    private static final String COORD_TRACE_ID = "4bf92f3577b34da6a3ce929d0e0e4736";
    private static final String COORD_SPAN_ID = "00f067aa0ba902b7";
    private static final String TRACEPARENT =
            "00-" + COORD_TRACE_ID + "-" + COORD_SPAN_ID + "-01";

    private InMemorySpanExporter spanExporter;
    private SdkTracerProvider tracerProvider;
    private TracingProvider provider;
    private WorkerTracingFilter filter;

    @BeforeEach
    void setUp() {
        spanExporter = InMemorySpanExporter.create();
        tracerProvider = SdkTracerProvider.builder()
                .addSpanProcessor(SimpleSpanProcessor.create(spanExporter))
                .build();
        provider = new OtelTracingProvider(tracerProvider.get("pangrova-worker-test"));
        filter = new WorkerTracingFilter(new TracingHttpFilter(provider));
    }

    @AfterEach
    void tearDown() {
        tracerProvider.close();
    }

    @Test
    void workerServerSpanParentsToCoordinatorInjectedTrace() {
        StubRequestContext request = new StubRequestContext("/v1/task", TRACEPARENT);
        filter.filter(request);
        filter.filter(request, new StubResponseContext(200));

        SpanData serverSpan = findSpan("POST /v1/task");
        assertThat(serverSpan.getSpanContext().getTraceId())
                .as("worker server span must join the coordinator's trace")
                .isEqualTo(COORD_TRACE_ID);
        assertThat(serverSpan.getParentSpanContext().getSpanId())
                .as("worker server span's parent must be the coordinator-injected span")
                .isEqualTo(COORD_SPAN_ID);
    }

    @Test
    void taskWorkNestsUnderActivatedServerSpan() {
        StubRequestContext request = new StubRequestContext("/v1/task", TRACEPARENT);
        filter.filter(request);

        // Simulate what TaskResource.submitTask does while the filter scope is active: it starts
        // its own span without passing an explicit parent. With activation this must attach to the
        // worker server span (same trace), proving native/MDC traceId will be non-zero too.
        Span taskSpan = provider.startSpan("task.submit", SpanKind.SERVER);
        taskSpan.end();

        filter.filter(request, new StubResponseContext(200));

        SpanData serverSpan = findSpan("POST /v1/task");
        SpanData childSpan = findSpan("task.submit");
        assertThat(childSpan.getSpanContext().getTraceId())
                .as("task work must share the worker server span's trace (== coordinator trace)")
                .isEqualTo(serverSpan.getSpanContext().getTraceId())
                .isEqualTo(COORD_TRACE_ID);
        assertThat(childSpan.getParentSpanContext().getSpanId())
                .as("task work must parent to the activated worker server span")
                .isEqualTo(serverSpan.getSpanContext().getSpanId());
    }

    @Test
    void withoutTraceparentWorkerStartsFreshRootTrace() {
        StubRequestContext request = new StubRequestContext("/v1/task", null);
        filter.filter(request);
        filter.filter(request, new StubResponseContext(200));

        SpanData serverSpan = findSpan("POST /v1/task");
        assertThat(serverSpan.getParentSpanContext().isValid())
                .as("no inbound traceparent → worker span is a fresh root")
                .isFalse();
        assertThat(serverSpan.getSpanContext().getTraceId())
                .isNotEqualTo("00000000000000000000000000000000");
    }

    private SpanData findSpan(String name) {
        List<SpanData> spans = spanExporter.getFinishedSpanItems();
        return spans.stream()
                .filter(s -> s.getName().equals(name))
                .findFirst()
                .orElseThrow(() -> new AssertionError("no span named " + name + " in " + spans));
    }

    // ── JAX-RS stubs ──

    private static final class StubRequestContext implements ContainerRequestContext {
        private final String path;
        private final Map<String, Object> properties = new HashMap<>();
        private final MultivaluedMap<String, String> headers = new MultivaluedHashMap<>();

        StubRequestContext(String path, String traceparent) {
            this.path = path;
            if (traceparent != null) {
                headers.putSingle("traceparent", traceparent);
            }
        }

        @Override public Object getProperty(String name) { return properties.get(name); }
        @Override public java.util.Collection<String> getPropertyNames() { return properties.keySet(); }
        @Override public void setProperty(String name, Object object) { properties.put(name, object); }
        @Override public void removeProperty(String name) { properties.remove(name); }
        @Override public String getMethod() { return "POST"; }
        @Override public MultivaluedMap<String, String> getHeaders() { return headers; }
        @Override public String getHeaderString(String name) { return headers.getFirst(name); }
        @Override public boolean containsHeaderString(
                String name, String value, java.util.function.Predicate<String> valuePredicate) {
            return headers.getOrDefault(name, List.of()).stream()
                    .anyMatch(valuePredicate);
        }
        @Override public UriInfo getUriInfo() { return new StubUriInfo(path); }

        @Override public void setRequestUri(URI requestUri) {}
        @Override public void setRequestUri(URI baseUri, URI requestUri) {}
        @Override public jakarta.ws.rs.core.Request getRequest() { return null; }
        @Override public void setMethod(String method) {}
        @Override public java.util.Date getDate() { return null; }
        @Override public java.util.Locale getLanguage() { return null; }
        @Override public int getLength() { return -1; }
        @Override public jakarta.ws.rs.core.MediaType getMediaType() { return null; }
        @Override public java.util.List<jakarta.ws.rs.core.MediaType> getAcceptableMediaTypes() { return List.of(); }
        @Override public java.util.List<java.util.Locale> getAcceptableLanguages() { return List.of(); }
        @Override public java.util.Map<String, jakarta.ws.rs.core.Cookie> getCookies() { return Map.of(); }
        @Override public boolean hasEntity() { return false; }
        @Override public java.io.InputStream getEntityStream() { return null; }
        @Override public void setEntityStream(java.io.InputStream input) {}
        @Override public jakarta.ws.rs.core.SecurityContext getSecurityContext() { return null; }
        @Override public void setSecurityContext(jakarta.ws.rs.core.SecurityContext context) {}
        @Override public void abortWith(jakarta.ws.rs.core.Response response) {}
    }

    private static final class StubResponseContext implements ContainerResponseContext {
        private final int status;

        StubResponseContext(int status) {
            this.status = status;
        }

        @Override public int getStatus() { return status; }

        @Override public void setStatus(int code) {}
        @Override public jakarta.ws.rs.core.Response.StatusType getStatusInfo() { return null; }
        @Override public void setStatusInfo(jakarta.ws.rs.core.Response.StatusType statusInfo) {}
        @Override public MultivaluedMap<String, Object> getHeaders() { return new MultivaluedHashMap<>(); }
        @Override public MultivaluedMap<String, String> getStringHeaders() { return new MultivaluedHashMap<>(); }
        @Override public String getHeaderString(String name) { return null; }
        @Override public boolean containsHeaderString(
                String name, String value, java.util.function.Predicate<String> valuePredicate) {
            return false;
        }
        @Override public java.util.Set<String> getAllowedMethods() { return java.util.Set.of(); }
        @Override public java.util.Date getDate() { return null; }
        @Override public java.util.Locale getLanguage() { return null; }
        @Override public int getLength() { return -1; }
        @Override public jakarta.ws.rs.core.MediaType getMediaType() { return null; }
        @Override public java.util.Map<String, jakarta.ws.rs.core.NewCookie> getCookies() { return Map.of(); }
        @Override public jakarta.ws.rs.core.EntityTag getEntityTag() { return null; }
        @Override public java.util.Date getLastModified() { return null; }
        @Override public URI getLocation() { return null; }
        @Override public java.util.Set<jakarta.ws.rs.core.Link> getLinks() { return java.util.Set.of(); }
        @Override public boolean hasLink(String relation) { return false; }
        @Override public jakarta.ws.rs.core.Link getLink(String relation) { return null; }
        @Override public jakarta.ws.rs.core.Link.Builder getLinkBuilder(String relation) { return null; }
        @Override public boolean hasEntity() { return false; }
        @Override public Object getEntity() { return null; }
        @Override public Class<?> getEntityClass() { return null; }
        @Override public java.lang.reflect.Type getEntityType() { return null; }
        @Override public void setEntity(Object entity) {}
        @Override public void setEntity(Object entity, java.lang.annotation.Annotation[] annotations,
                                        jakarta.ws.rs.core.MediaType mediaType) {}
        @Override public java.lang.annotation.Annotation[] getEntityAnnotations() {
            return new java.lang.annotation.Annotation[0];
        }
        @Override public java.io.OutputStream getEntityStream() { return null; }
        @Override public void setEntityStream(java.io.OutputStream outputStream) {}
    }

    private static final class StubUriInfo implements UriInfo {
        private final String path;

        StubUriInfo(String path) {
            this.path = path;
        }

        @Override public String getPath() { return path; }
        @Override public String getPath(boolean decode) { return path; }
        @Override public URI getRequestUri() { return URI.create("http://localhost" + path); }
        @Override public URI getAbsolutePath() { return URI.create("http://localhost" + path); }
        @Override public URI getBaseUri() { return URI.create("http://localhost/"); }

        @Override public java.util.List<jakarta.ws.rs.core.PathSegment> getPathSegments() { return List.of(); }
        @Override public java.util.List<jakarta.ws.rs.core.PathSegment> getPathSegments(boolean decode) { return List.of(); }
        @Override public jakarta.ws.rs.core.UriBuilder getRequestUriBuilder() { return null; }
        @Override public jakarta.ws.rs.core.UriBuilder getAbsolutePathBuilder() { return null; }
        @Override public jakarta.ws.rs.core.UriBuilder getBaseUriBuilder() { return null; }
        @Override public MultivaluedMap<String, String> getPathParameters() { return new MultivaluedHashMap<>(); }
        @Override public MultivaluedMap<String, String> getPathParameters(boolean decode) { return new MultivaluedHashMap<>(); }
        @Override public MultivaluedMap<String, String> getQueryParameters() { return new MultivaluedHashMap<>(); }
        @Override public MultivaluedMap<String, String> getQueryParameters(boolean decode) { return new MultivaluedHashMap<>(); }
        @Override public java.util.List<String> getMatchedURIs() { return List.of(); }
        @Override public String getMatchedResourceTemplate() { return path; }
        @Override public java.util.List<String> getMatchedURIs(boolean decode) { return List.of(); }
        @Override public java.util.List<Object> getMatchedResources() { return List.of(); }
        @Override public URI resolve(URI uri) { return uri; }
        @Override public URI relativize(URI uri) { return uri; }
    }
}
