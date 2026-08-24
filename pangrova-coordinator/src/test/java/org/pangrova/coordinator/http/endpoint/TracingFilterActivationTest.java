package org.pangrova.coordinator.http.endpoint;

import static org.assertj.core.api.Assertions.assertThat;

import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import jakarta.ws.rs.container.ContainerRequestContext;
import jakarta.ws.rs.container.ContainerResponseContext;
import jakarta.ws.rs.core.MultivaluedHashMap;
import jakarta.ws.rs.core.MultivaluedMap;
import jakarta.ws.rs.core.UriInfo;
import org.pangrova.observability.tracing.TracingHttpFilter;
import org.pangrova.spi.observability.Context;
import org.pangrova.spi.observability.Scope;
import org.pangrova.spi.observability.Span;
import org.pangrova.spi.observability.SpanKind;
import org.pangrova.spi.observability.StatusCode;
import org.pangrova.spi.observability.TextMapGetter;
import org.pangrova.spi.observability.TextMapSetter;
import org.pangrova.spi.observability.TracingProvider;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Verifies {@link TracingFilter} honours the span <em>activation</em> contract (F-CHAIN-OBS-01):
 * the request filter must {@code makeCurrent()} the server span, and the response filter must close
 * that scope BEFORE ending the span. Before the fix the filter only stored the span in a request
 * property without activating it, so {@code Context.current()} stayed empty and nested spans detached.
 *
 * <p>This test uses SPI-level recording fakes (no tracing SDK) to assert the wiring/ordering contract;
 * the end-to-end "nested span attaches to the activated parent" semantics is proven against the real
 * OpenTelemetry provider in {@code pangrova-observability}'s {@code SpanActivationTest}.
 */
class TracingFilterActivationTest {

    private final List<String> events = new ArrayList<>();
    private RecordingSpan serverSpan;
    private TracingFilter filter;

    @BeforeEach
    void setUp() {
        events.clear();
        filter = new TracingFilter(new TracingHttpFilter(new RecordingProvider()));
    }

    @Test
    void requestFilterActivatesServerSpan() {
        StubRequestContext request = new StubRequestContext("/v1/query/submit");
        filter.filter(request);

        assertThat(serverSpan).as("a server span must have been created").isNotNull();
        assertThat(serverSpan.makeCurrentCount)
                .as("the request filter must activate (makeCurrent) the server span exactly once")
                .isEqualTo(1);
        assertThat(events).containsExactly("makeCurrent");
        assertThat(serverSpan.ended).as("span must not end on the request leg").isFalse();
    }

    @Test
    void responseFilterClosesScopeBeforeEndingSpan() {
        StubRequestContext request = new StubRequestContext("/v1/query/submit");
        filter.filter(request);
        filter.filter(request, new StubResponseContext(200));

        // The scope must be deactivated before the span ends — otherwise the next request on this
        // thread would observe a stale "current" span.
        assertThat(events).containsExactly("makeCurrent", "scope.close", "span.end");
        assertThat(serverSpan.ended).isTrue();
    }

    // ── SPI recording fakes ──

    private final class RecordingProvider implements TracingProvider {
        @Override
        public Span startSpan(String name, SpanKind kind) {
            return register(new RecordingSpan());
        }

        @Override
        public Span startSpan(String name, SpanKind kind, Context parent) {
            return register(new RecordingSpan());
        }

        private Span register(RecordingSpan span) {
            serverSpan = span;
            return span;
        }

        @Override
        public Span currentSpan() {
            return new RecordingSpan();
        }

        @Override
        public <C> void inject(Context context, C carrier, TextMapSetter<C> setter) {
        }

        @Override
        public <C> Context extract(Context context, C carrier, TextMapGetter<C> getter) {
            return null; // parent context unused by the fake startSpan
        }
    }

    private final class RecordingSpan implements Span {
        int makeCurrentCount;
        boolean ended;

        @Override public Span setAttribute(String key, String value) { return this; }
        @Override public Span setAttribute(String key, long value) { return this; }
        @Override public Span setStatus(StatusCode statusCode, String description) { return this; }
        @Override public Span recordException(Throwable exception) { return this; }

        @Override
        public Scope makeCurrent() {
            makeCurrentCount++;
            events.add("makeCurrent");
            return () -> events.add("scope.close");
        }

        @Override
        public void end() {
            ended = true;
            events.add("span.end");
        }
    }

    // ── JAX-RS stubs ──

    private static final class StubRequestContext implements ContainerRequestContext {
        private final String path;
        private final Map<String, Object> properties = new HashMap<>();
        private final MultivaluedMap<String, String> headers = new MultivaluedHashMap<>();

        StubRequestContext(String path) {
            this.path = path;
        }

        @Override public Object getProperty(String name) { return properties.get(name); }
        @Override public java.util.Collection<String> getPropertyNames() { return properties.keySet(); }
        @Override public void setProperty(String name, Object object) { properties.put(name, object); }
        @Override public void removeProperty(String name) { properties.remove(name); }
        @Override public String getMethod() { return "POST"; }
        @Override public MultivaluedMap<String, String> getHeaders() { return headers; }
        @Override public String getHeaderString(String name) { return headers.getFirst(name); }
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
        @Override public java.util.List<String> getMatchedURIs(boolean decode) { return List.of(); }
        @Override public java.util.List<Object> getMatchedResources() { return List.of(); }
        @Override public URI resolve(URI uri) { return uri; }
        @Override public URI relativize(URI uri) { return uri; }
    }
}
