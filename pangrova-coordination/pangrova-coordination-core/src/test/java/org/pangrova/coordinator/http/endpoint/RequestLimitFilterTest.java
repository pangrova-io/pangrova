package org.pangrova.coordinator.http.endpoint;

import static org.assertj.core.api.Assertions.assertThat;

import jakarta.ws.rs.core.MultivaluedHashMap;
import jakarta.ws.rs.core.MultivaluedMap;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.core.UriInfo;
import org.pangrova.coordinator.http.RequestLimitConfig;
import org.pangrova.protocol.rest.worker.WorkerRegistrationRequest;
import org.pangrova.protocol.rest.task.TaskStatusReport;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;

/**
 * Unit tests for {@link RequestLimitFilter}.
 * Verifies request body size limit (413) and per-IP rate limiting (429).
 */
class RequestLimitFilterTest {

    private static final int MAX_SIZE = 1024; // 1 KB for testing
    private static final int MAX_REQUESTS = 5; // 5 requests/minute for testing

    private RequestLimitFilter filter;

    @BeforeEach
    void setUp() {
        filter = new RequestLimitFilter(new RequestLimitConfig(MAX_SIZE, MAX_REQUESTS));
    }

    @Test
    void allowsRequestWithinSizeLimit() throws IOException {
        StubRequestContext ctx = new StubRequestContext("/v1/query/submit");
        ctx.setContentLength(512);
        ctx.setEntityStream(new ByteArrayInputStream(new byte[512]));
        ctx.setProperty(RequestPeerAddressFilter.PEER_PROPERTY, "10.0.0.1");

        filter.filter(ctx);

        assertThat(ctx.isAborted()).isFalse();
    }

    @Test
    void rejectsRequestExceedingContentLengthWith413() throws IOException {
        StubRequestContext ctx = new StubRequestContext("/v1/query/submit");
        ctx.setContentLength(MAX_SIZE + 1);
        ctx.setEntityStream(new ByteArrayInputStream(new byte[MAX_SIZE + 1]));
        ctx.setProperty(RequestPeerAddressFilter.PEER_PROPERTY, "10.0.0.2");

        filter.filter(ctx);

        assertThat(ctx.isAborted()).isTrue();
        assertThat(ctx.getAbortedStatus()).isEqualTo(413);
    }

    @Test
    void registrationAcceptsCanonicalInventoryEnvelopeButOtherRoutesKeepOriginalLimit() throws IOException {
        int bodyBytes = MAX_SIZE + 1;
        StubRequestContext registration = new StubRequestContext("/v1/workers/register");
        authenticatedInternal(registration);
        registration.setContentLength(bodyBytes);
        registration.setEntityStream(new ByteArrayInputStream(new byte[bodyBytes]));
        filter.filter(registration);
        assertThat(registration.isAborted()).isFalse();
        assertThat(registration.getEntityStream().readAllBytes()).hasSize(bodyBytes);

        StubRequestContext heartbeat = new StubRequestContext("/v1/worker/heartbeat");
        heartbeat.setContentLength(bodyBytes);
        heartbeat.setEntityStream(new ByteArrayInputStream(new byte[bodyBytes]));
        filter.filter(heartbeat);
        assertThat(heartbeat.getAbortedStatus()).isEqualTo(413);

        StubRequestContext otherMethod = new StubRequestContext("/v1/workers/register");
        otherMethod.setMethod("PUT");
        otherMethod.setContentLength(bodyBytes);
        filter.filter(otherMethod);
        assertThat(otherMethod.getAbortedStatus()).isEqualTo(413);
    }

    @Test
    void terminalReportEnvelopeHasItsOwnBoundWhilePublicRoutesKeepTheDefault() throws IOException {
        int bodyBytes = RequestLimitConfig.DEFAULT_MAX_REQUEST_SIZE_BYTES + 24_071;
        StubRequestContext status = new StubRequestContext("/v1/task/status-report");
        authenticatedInternal(status);
        status.setContentLength(bodyBytes);
        status.setEntityStream(new ByteArrayInputStream(new byte[bodyBytes]));
        filter.filter(status);
        assertThat(status.isAborted()).isFalse();
        assertThat(status.getEntityStream().readAllBytes()).hasSize(bodyBytes);

        StubRequestContext publicRequest = new StubRequestContext("/v1/query/submit");
        publicRequest.setContentLength(bodyBytes);
        filter.filter(publicRequest);
        assertThat(publicRequest.getAbortedStatus()).isEqualTo(413);

        StubRequestContext oversized = new StubRequestContext("/v1/task/status-report");
        authenticatedInternal(oversized);
        oversized.setContentLength(TaskStatusReport.MAX_HTTP_BODY_BYTES + 1);
        filter.filter(oversized);
        assertThat(oversized.getAbortedStatus()).isEqualTo(413);
    }

    @Test
    void registrationBodyBoundAppliesToDeclaredAndStreamingLengths() throws IOException {
        int exact = WorkerRegistrationRequest.MAX_REGISTRATION_BODY_BYTES;
        StubRequestContext declaredTooLarge = new StubRequestContext("v1/workers/register");
        authenticatedInternal(declaredTooLarge);
        declaredTooLarge.setContentLength(exact + 1);
        filter.filter(declaredTooLarge);
        assertThat(declaredTooLarge.getAbortedStatus()).isEqualTo(413);

        StubRequestContext unknownLength = new StubRequestContext("v1/workers/register");
        authenticatedInternal(unknownLength);
        unknownLength.setEntityStream(new ByteArrayInputStream(new byte[exact + 1]));
        filter.filter(unknownLength);
        assertThat(unknownLength.isAborted()).isFalse();
        assertThat(unknownLength.getEntityStream().readNBytes(exact)).hasSize(exact);
        org.assertj.core.api.Assertions.assertThatThrownBy(
                () -> unknownLength.getEntityStream().read())
                .isInstanceOf(jakarta.ws.rs.WebApplicationException.class);
        org.assertj.core.api.Assertions.assertThatThrownBy(
                () -> unknownLength.getEntityStream().read())
                .isInstanceOf(jakarta.ws.rs.WebApplicationException.class);

        StubRequestContext exactLength = new StubRequestContext("v1/workers/register");
        authenticatedInternal(exactLength);
        exactLength.setEntityStream(new ByteArrayInputStream(new byte[exact]));
        filter.filter(exactLength);
        assertThat(exactLength.getEntityStream().readAllBytes()).hasSize(exact);
    }

    @Test
    void rejectsRequestWhenRateLimitExceeded() throws IOException {
        String clientIp = "192.168.1.100";

        for (int i = 0; i < MAX_REQUESTS; i++) {
            StubRequestContext ctx = new StubRequestContext("/v1/query/submit");
            ctx.setContentLength(10);
            ctx.setEntityStream(new ByteArrayInputStream(new byte[10]));
            ctx.setProperty(RequestPeerAddressFilter.PEER_PROPERTY, clientIp);
            filter.filter(ctx);
            assertThat(ctx.isAborted()).isFalse();
        }

        // Next request should be rejected
        StubRequestContext ctx = new StubRequestContext("/v1/query/submit");
        ctx.setContentLength(10);
        ctx.setEntityStream(new ByteArrayInputStream(new byte[10]));
        ctx.setProperty(RequestPeerAddressFilter.PEER_PROPERTY, clientIp);
        filter.filter(ctx);

        assertThat(ctx.isAborted()).isTrue();
        assertThat(ctx.getAbortedStatus()).isEqualTo(429);
    }

    @Test
    void rateLimitIsPerIp() throws IOException {
        String ip1 = "10.0.0.1";
        String ip2 = "10.0.0.2";

        // Exhaust rate limit for ip1
        for (int i = 0; i <= MAX_REQUESTS; i++) {
            StubRequestContext ctx = new StubRequestContext("/v1/query/submit");
            ctx.setContentLength(10);
            ctx.setEntityStream(new ByteArrayInputStream(new byte[10]));
            ctx.setProperty(RequestPeerAddressFilter.PEER_PROPERTY, ip1);
            filter.filter(ctx);
        }

        // ip2 should still be allowed
        StubRequestContext ctx = new StubRequestContext("/v1/query/submit");
        ctx.setContentLength(10);
        ctx.setEntityStream(new ByteArrayInputStream(new byte[10]));
        ctx.setProperty(RequestPeerAddressFilter.PEER_PROPERTY, ip2);
        filter.filter(ctx);

        assertThat(ctx.isAborted()).isFalse();
    }

    @Test
    void healthCheckExemptFromRateLimit() throws IOException {
        // Exhaust rate limit
        for (int i = 0; i <= MAX_REQUESTS; i++) {
            StubRequestContext ctx = new StubRequestContext("/v1/query/submit");
            ctx.setContentLength(10);
            ctx.setEntityStream(new ByteArrayInputStream(new byte[10]));
            filter.filter(ctx);
        }

        // Health check should still pass
        StubRequestContext healthCtx = new StubRequestContext("v1/health");
        healthCtx.setContentLength(0);
        filter.filter(healthCtx);

        assertThat(healthCtx.isAborted()).isFalse();
    }

    @Test
    void healthCheckExemptFromSizeLimit() throws IOException {
        StubRequestContext ctx = new StubRequestContext("v1/health");
        ctx.setContentLength(MAX_SIZE + 100);
        ctx.setEntityStream(new ByteArrayInputStream(new byte[MAX_SIZE + 100]));

        filter.filter(ctx);

        assertThat(ctx.isAborted()).isFalse();
    }

    @Test
    void sizeLimitedInputStreamEnforcesLimit() throws IOException {
        byte[] data = new byte[MAX_SIZE + 100];
        InputStream limited = new RequestLimitFilter.SizeLimitedInputStream(
                new ByteArrayInputStream(data), MAX_SIZE);

        byte[] buf = new byte[MAX_SIZE + 100];
        int totalRead = 0;
        int n;
        while ((n = limited.read(buf, totalRead, buf.length - totalRead)) != -1) {
            totalRead += n;
            if (totalRead >= MAX_SIZE) {
                break;
            }
        }

        assertThat(totalRead).isEqualTo(MAX_SIZE);

        // Next read should throw
        org.assertj.core.api.Assertions.assertThatThrownBy(() -> limited.read(buf, MAX_SIZE, 1))
                .isInstanceOf(jakarta.ws.rs.WebApplicationException.class)
                .hasMessageContaining("exceeds maximum size")
                .satisfies(failure -> assertThat(((jakarta.ws.rs.WebApplicationException) failure)
                        .getResponse().getStatus()).isEqualTo(413));
        org.assertj.core.api.Assertions.assertThatThrownBy(limited::read)
                .isInstanceOf(jakarta.ws.rs.WebApplicationException.class);
    }

    @Test
    void exactSizeEofAndZeroLengthReadRespectInputStreamContract() throws IOException {
        var limited = new RequestLimitFilter.SizeLimitedInputStream(new ByteArrayInputStream(new byte[MAX_SIZE]), MAX_SIZE);
        assertThat(limited.readNBytes(MAX_SIZE)).hasSize(MAX_SIZE);
        assertThat(limited.read(new byte[1], 0, 0)).isZero();
        assertThat(limited.read(new byte[1], 0, 1)).isEqualTo(-1);
        assertThat(limited.read()).isEqualTo(-1);
    }

    @Test
    void getRequestCountReturnsZeroForUnknownIp() {
        assertThat(filter.getRequestCount("never-seen")).isEqualTo(0);
    }

    @Test
    void getRequestCountTracksRequests() throws IOException {
        String ip = "10.0.0.99";
        StubRequestContext ctx = new StubRequestContext("/v1/query/submit");
        ctx.setContentLength(10);
        ctx.setEntityStream(new ByteArrayInputStream(new byte[10]));
        ctx.setProperty(RequestPeerAddressFilter.PEER_PROPERTY, ip);

        filter.filter(ctx);

        assertThat(filter.getRequestCount(ip)).isEqualTo(1);
    }

    private static void authenticatedInternal(StubRequestContext context) {
        context.setProperty(AuthenticationFilter.INTERNAL_IDENTITY_PROPERTY,
                AuthenticationFilter.InternalServiceIdentity.AUTHENTICATED);
    }

    /**
     * Minimal ContainerRequestContext for testing filter logic without a full JAX-RS container.
     * Provides real behavior for the methods that RequestLimitFilter actually invokes.
     */
    private static final class StubRequestContext implements jakarta.ws.rs.container.ContainerRequestContext {

        private final String path;
        private String method = "POST";
        private final MultivaluedMap<String, String> headers = new MultivaluedHashMap<>();
        private int contentLength = -1;
        private InputStream entityStream;
        private Response abortResponse;
        private final java.util.Map<String, Object> properties = new java.util.HashMap<>();

        StubRequestContext(String path) {
            this.path = path;
        }

        void setContentLength(int length) {
            this.contentLength = length;
        }

        @Override
        public void setMethod(String method) {
            this.method = method;
        }

        void addHeader(String name, String value) {
            headers.add(name, value);
        }

        boolean isAborted() {
            return abortResponse != null;
        }

        int getAbortedStatus() {
            return abortResponse != null ? abortResponse.getStatus() : -1;
        }

        @Override
        public UriInfo getUriInfo() {
            return new StubUriInfo(path);
        }

        @Override
        public String getHeaderString(String name) {
            return headers.getFirst(name);
        }

        @Override
        public boolean containsHeaderString(
                String name, String value, java.util.function.Predicate<String> valuePredicate) {
            return headers.getOrDefault(name, java.util.List.of()).stream()
                    .anyMatch(valuePredicate);
        }

        @Override
        public int getLength() {
            return contentLength;
        }

        @Override
        public InputStream getEntityStream() {
            return entityStream;
        }

        @Override
        public void setEntityStream(InputStream input) {
            this.entityStream = input;
        }

        @Override
        public void abortWith(Response response) {
            this.abortResponse = response;
        }

        // Unused interface methods — required by ContainerRequestContext contract
        @Override public Object getProperty(String name) { return properties.get(name); }
        @Override public java.util.Collection<String> getPropertyNames() { return properties.keySet(); }
        @Override public void setProperty(String name, Object object) { properties.put(name, object); }
        @Override public void removeProperty(String name) { properties.remove(name); }
        @Override public void setRequestUri(URI requestUri) {}
        @Override public void setRequestUri(URI baseUri, URI requestUri) {}
        @Override public jakarta.ws.rs.core.Request getRequest() { return null; }
        @Override public String getMethod() { return method; }
        @Override public MultivaluedMap<String, String> getHeaders() { return headers; }
        @Override public java.util.Date getDate() { return null; }
        @Override public java.util.Locale getLanguage() { return null; }
        @Override public jakarta.ws.rs.core.MediaType getMediaType() { return null; }
        @Override public java.util.List<jakarta.ws.rs.core.MediaType> getAcceptableMediaTypes() { return java.util.List.of(); }
        @Override public java.util.List<java.util.Locale> getAcceptableLanguages() { return java.util.List.of(); }
        @Override public java.util.Map<String, jakarta.ws.rs.core.Cookie> getCookies() { return java.util.Map.of(); }
        @Override public boolean hasEntity() { return entityStream != null; }
        @Override public jakarta.ws.rs.core.SecurityContext getSecurityContext() { return null; }
        @Override public void setSecurityContext(jakarta.ws.rs.core.SecurityContext context) {}
    }

    private static final class StubUriInfo implements UriInfo {
        private final String path;

        StubUriInfo(String path) {
            this.path = path;
        }

        @Override
        public String getPath() {
            return path;
        }

        @Override public String getPath(boolean decode) { return path; }
        @Override public java.util.List<jakarta.ws.rs.core.PathSegment> getPathSegments() { return java.util.List.of(); }
        @Override public java.util.List<jakarta.ws.rs.core.PathSegment> getPathSegments(boolean decode) { return java.util.List.of(); }
        @Override public URI getRequestUri() { return URI.create("http://localhost/" + path); }
        @Override public jakarta.ws.rs.core.UriBuilder getRequestUriBuilder() { return null; }
        @Override public URI getAbsolutePath() { return URI.create("http://localhost/" + path); }
        @Override public jakarta.ws.rs.core.UriBuilder getAbsolutePathBuilder() { return null; }
        @Override public URI getBaseUri() { return URI.create("http://localhost/"); }
        @Override public jakarta.ws.rs.core.UriBuilder getBaseUriBuilder() { return null; }
        @Override public MultivaluedMap<String, String> getPathParameters() { return new MultivaluedHashMap<>(); }
        @Override public MultivaluedMap<String, String> getPathParameters(boolean decode) { return new MultivaluedHashMap<>(); }
        @Override public MultivaluedMap<String, String> getQueryParameters() { return new MultivaluedHashMap<>(); }
        @Override public MultivaluedMap<String, String> getQueryParameters(boolean decode) { return new MultivaluedHashMap<>(); }
        @Override public java.util.List<String> getMatchedURIs() { return java.util.List.of(); }
        @Override public java.util.List<String> getMatchedURIs(boolean decode) { return java.util.List.of(); }
        @Override public java.util.List<Object> getMatchedResources() { return java.util.List.of(); }
        @Override public String getMatchedResourceTemplate() { return path; }
        @Override public URI resolve(URI uri) { return uri; }
        @Override public URI relativize(URI uri) { return uri; }
    }
}
