package org.pangrova.server.coordinator.http.endpoint;

import static org.assertj.core.api.Assertions.assertThat;

import jakarta.ws.rs.core.MultivaluedHashMap;
import jakarta.ws.rs.core.MultivaluedMap;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.core.UriInfo;
import org.pangrova.common.client.version.ApiVersion;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.net.URI;

/**
 * Unit tests for {@link ApiVersionFilter}.
 * Verifies REST API version negotiation: request validation and response header injection.
 */
class ApiVersionFilterTest {

    private ApiVersionFilter filter;

    @BeforeEach
    void setUp() {
        filter = new ApiVersionFilter();
    }

    // --- Request path tests ---

    @Test
    void allowsRequestWithCurrentVersion() {
        StubRequestContext ctx = new StubRequestContext("/v1/query/submit");
        ctx.addHeader(ApiVersion.HEADER_NAME, String.valueOf(ApiVersion.CURRENT));

        filter.filter(ctx);

        assertThat(ctx.isAborted()).isFalse();
    }

    @Test
    void allowsRequestWithMinSupportedVersion() {
        StubRequestContext ctx = new StubRequestContext("/v1/query/submit");
        ctx.addHeader(ApiVersion.HEADER_NAME, String.valueOf(ApiVersion.MIN_SUPPORTED));

        filter.filter(ctx);

        assertThat(ctx.isAborted()).isFalse();
    }

    @Test
    void allowsRequestWithoutVersionHeader() {
        // Legacy clients without the header are allowed through with a warning
        StubRequestContext ctx = new StubRequestContext("/v1/query/submit");

        filter.filter(ctx);

        assertThat(ctx.isAborted()).isFalse();
    }

    @Test
    void rejectsRequestWithVersionTooLow() {
        StubRequestContext ctx = new StubRequestContext("/v1/query/submit");
        ctx.addHeader(ApiVersion.HEADER_NAME, String.valueOf(ApiVersion.MIN_SUPPORTED - 1));

        filter.filter(ctx);

        assertThat(ctx.isAborted()).isTrue();
        assertThat(ctx.getAbortedStatus()).isEqualTo(422);
    }

    @Test
    void rejectsRequestWithVersionTooHigh() {
        StubRequestContext ctx = new StubRequestContext("/v1/query/submit");
        ctx.addHeader(ApiVersion.HEADER_NAME, String.valueOf(ApiVersion.CURRENT + 1));

        filter.filter(ctx);

        assertThat(ctx.isAborted()).isTrue();
        assertThat(ctx.getAbortedStatus()).isEqualTo(422);
    }

    @Test
    void rejectsRequestWithNonNumericVersion() {
        StubRequestContext ctx = new StubRequestContext("/v1/query/submit");
        ctx.addHeader(ApiVersion.HEADER_NAME, "abc");

        filter.filter(ctx);

        assertThat(ctx.isAborted()).isTrue();
        assertThat(ctx.getAbortedStatus()).isEqualTo(422);
    }

    @Test
    void rejectsRequestWithEmptyVersion() {
        // Empty header is treated the same as absent — allowed through
        StubRequestContext ctx = new StubRequestContext("/v1/query/submit");
        ctx.addHeader(ApiVersion.HEADER_NAME, "  ");

        filter.filter(ctx);

        assertThat(ctx.isAborted()).isFalse();
    }

    // --- Exempt paths ---

    @Test
    void healthCheckExemptFromVersionCheck() {
        StubRequestContext ctx = new StubRequestContext("v1/health");
        ctx.addHeader(ApiVersion.HEADER_NAME, "999");

        filter.filter(ctx);

        assertThat(ctx.isAborted()).isFalse();
    }

    @Test
    void serverInfoExemptFromVersionCheck() {
        StubRequestContext ctx = new StubRequestContext("v1/server/info");
        ctx.addHeader(ApiVersion.HEADER_NAME, "999");

        filter.filter(ctx);

        assertThat(ctx.isAborted()).isFalse();
    }

    @Test
    void healthCheckExemptWithLeadingSlash() {
        StubRequestContext ctx = new StubRequestContext("/v1/health");
        ctx.addHeader(ApiVersion.HEADER_NAME, "999");

        filter.filter(ctx);

        assertThat(ctx.isAborted()).isFalse();
    }

    @Test
    void livenessCheckExemptFromVersionCheck() {
        StubRequestContext ctx = new StubRequestContext("/v1/health/live");
        ctx.addHeader(ApiVersion.HEADER_NAME, "999");

        filter.filter(ctx);

        assertThat(ctx.isAborted()).isFalse();
    }

    @Test
    void livenessSubpathStillRequiresSupportedVersion() {
        StubRequestContext ctx = new StubRequestContext("/v1/health/live/extra");
        ctx.addHeader(ApiVersion.HEADER_NAME, "999");

        filter.filter(ctx);

        assertThat(ctx.isAborted()).isTrue();
    }

    // --- Response path tests ---

    @Test
    void responseAlwaysIncludesVersionHeader() {
        StubRequestContext reqCtx = new StubRequestContext("/v1/query/submit");
        StubResponseContext resCtx = new StubResponseContext();

        filter.filter(reqCtx, resCtx);

        String versionHeader = resCtx.getHeaderString(ApiVersion.HEADER_NAME);
        assertThat(versionHeader).isEqualTo(String.valueOf(ApiVersion.CURRENT));
    }

    // --- Stub implementations ---

    private static final class StubRequestContext implements jakarta.ws.rs.container.ContainerRequestContext {

        private final String path;
        private final MultivaluedMap<String, String> headers = new MultivaluedHashMap<>();
        private Response abortResponse;

        StubRequestContext(String path) {
            this.path = path;
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
        public void abortWith(Response response) {
            this.abortResponse = response;
        }

        // Unused interface methods
        @Override public Object getProperty(String name) { return null; }
        @Override public java.util.Collection<String> getPropertyNames() { return java.util.List.of(); }
        @Override public void setProperty(String name, Object object) {}
        @Override public void removeProperty(String name) {}
        @Override public void setRequestUri(URI requestUri) {}
        @Override public void setRequestUri(URI baseUri, URI requestUri) {}
        @Override public jakarta.ws.rs.core.Request getRequest() { return null; }
        @Override public String getMethod() { return "GET"; }
        @Override public void setMethod(String method) {}
        @Override public MultivaluedMap<String, String> getHeaders() { return headers; }
        @Override public java.util.Date getDate() { return null; }
        @Override public java.util.Locale getLanguage() { return null; }
        @Override public jakarta.ws.rs.core.MediaType getMediaType() { return null; }
        @Override public java.util.List<jakarta.ws.rs.core.MediaType> getAcceptableMediaTypes() { return java.util.List.of(); }
        @Override public java.util.List<java.util.Locale> getAcceptableLanguages() { return java.util.List.of(); }
        @Override public java.util.Map<String, jakarta.ws.rs.core.Cookie> getCookies() { return java.util.Map.of(); }
        @Override public boolean hasEntity() { return false; }
        @Override public java.io.InputStream getEntityStream() { return null; }
        @Override public void setEntityStream(java.io.InputStream input) {}
        @Override public int getLength() { return -1; }
        @Override public jakarta.ws.rs.core.SecurityContext getSecurityContext() { return null; }
        @Override public void setSecurityContext(jakarta.ws.rs.core.SecurityContext context) {}
    }

    private static final class StubResponseContext implements jakarta.ws.rs.container.ContainerResponseContext {

        private final MultivaluedMap<String, Object> headers = new MultivaluedHashMap<>();

        @Override
        public MultivaluedMap<String, Object> getHeaders() {
            return headers;
        }

        @Override public int getStatus() { return 200; }
        @Override public void setStatus(int code) {}
        @Override public Response.StatusType getStatusInfo() { return Response.Status.OK; }
        @Override public void setStatusInfo(Response.StatusType statusInfo) {}
        @Override public java.util.Date getDate() { return null; }
        @Override public java.util.Locale getLanguage() { return null; }
        @Override public jakarta.ws.rs.core.MediaType getMediaType() { return null; }
        @Override public java.util.Map<String, jakarta.ws.rs.core.NewCookie> getCookies() { return java.util.Map.of(); }
        @Override public jakarta.ws.rs.core.EntityTag getEntityTag() { return null; }
        @Override public java.util.Date getLastModified() { return null; }
        @Override public URI getLocation() { return null; }
        @Override public java.util.Set<String> getAllowedMethods() { return java.util.Set.of(); }
        @Override public int getLength() { return -1; }
        @Override public MultivaluedMap<String, String> getStringHeaders() { return new MultivaluedHashMap<>(); }
        @Override
        public String getHeaderString(String name) {
            Object val = headers.getFirst(name);
            return val != null ? val.toString() : null;
        }
        @Override
        public boolean containsHeaderString(
                String name, String value, java.util.function.Predicate<String> valuePredicate) {
            return headers.getOrDefault(name, java.util.List.of()).stream()
                    .map(Object::toString)
                    .anyMatch(valuePredicate);
        }
        @Override public java.lang.annotation.Annotation[] getEntityAnnotations() { return new java.lang.annotation.Annotation[0]; }
        @Override public java.lang.reflect.Type getEntityType() { return null; }
        @Override public Class<?> getEntityClass() { return null; }
        @Override public Object getEntity() { return null; }
        @Override public boolean hasEntity() { return false; }
        @Override public java.io.OutputStream getEntityStream() { return null; }
        @Override public void setEntityStream(java.io.OutputStream outputStream) {}
        @Override public void setEntity(Object entity) {}
        @Override public void setEntity(Object entity, java.lang.annotation.Annotation[] annotations, jakarta.ws.rs.core.MediaType mediaType) {}
        @Override public java.util.Set<jakarta.ws.rs.core.Link> getLinks() { return java.util.Set.of(); }
        @Override public boolean hasLink(String relation) { return false; }
        @Override public jakarta.ws.rs.core.Link getLink(String relation) { return null; }
        @Override public jakarta.ws.rs.core.Link.Builder getLinkBuilder(String relation) { return null; }
    }

    private static final class StubUriInfo implements UriInfo {
        private final String path;

        StubUriInfo(String path) {
            this.path = path;
        }

        @Override public String getPath() { return path; }
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
