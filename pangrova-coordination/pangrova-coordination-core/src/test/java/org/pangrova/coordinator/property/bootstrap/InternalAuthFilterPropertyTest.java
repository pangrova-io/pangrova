package org.pangrova.coordinator.property.bootstrap;

import net.jqwik.api.Arbitraries;
import net.jqwik.api.Arbitrary;
import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.Provide;
import org.pangrova.coordinator.http.endpoint.AuthenticationFilter;
import org.pangrova.security.authn.AuthenticationManager;
import org.pangrova.spi.security.authn.AuthenticationRequest;
import org.pangrova.spi.security.authn.AuthenticationResult;

import jakarta.ws.rs.container.ContainerRequestContext;
import jakarta.ws.rs.core.MultivaluedHashMap;
import jakarta.ws.rs.core.MultivaluedMap;
import jakarta.ws.rs.core.UriInfo;

import java.net.URI;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Property 12: Internal authentication closure.
 *
 * <p>For any HTTP request when auth is enabled: a valid internal token
 * bypasses external auth; an invalid or missing token is rejected.</p>
 *
 * <p><b>Validates: Requirement 14.3</b></p>
 */
class InternalAuthFilterPropertyTest {

    private static final String VALID_SECRET = "test-internal-secret-42";

    @Property(tries = 50)
    void validInternalTokenBypassesExternalAuthOnInternalRoutes(
            @ForAll("internalPaths") String path) {

        var rejectingAuthManager = new RejectAllAuthenticationManager();
        var filter = new AuthenticationFilter(rejectingAuthManager, VALID_SECRET);
        var ctx = new StubRequestContext(path, "Bearer " + VALID_SECRET);

        filter.filter(ctx);

        assertThat(ctx.aborted)
                .as("Valid internal token must bypass external auth on internal routes")
                .isFalse();
    }

    @Property(tries = 50)
    void validInternalTokenRejectedOnUserFacingRoutes(
            @ForAll("userFacingPaths") String path) {

        var rejectingAuthManager = new RejectAllAuthenticationManager();
        var filter = new AuthenticationFilter(rejectingAuthManager, VALID_SECRET);
        var ctx = new StubRequestContext(path, "Bearer " + VALID_SECRET);

        filter.filter(ctx);

        assertThat(ctx.aborted)
                .as("Valid internal token must be rejected on user-facing routes (route isolation)")
                .isTrue();
    }

    @Property(tries = 50)
    void invalidTokenIsRejected(
            @ForAll("invalidTokens") String token,
            @ForAll("requestPaths") String path) {

        var rejectingAuthManager = new RejectAllAuthenticationManager();
        var filter = new AuthenticationFilter(rejectingAuthManager, VALID_SECRET);
        var ctx = new StubRequestContext(path, token);

        filter.filter(ctx);

        assertThat(ctx.aborted)
                .as("Invalid token must be rejected")
                .isTrue();
    }

    @Property(tries = 20)
    void noTokenIsRejected(@ForAll("requestPaths") String path) {
        var rejectingAuthManager = new RejectAllAuthenticationManager();
        var filter = new AuthenticationFilter(rejectingAuthManager, VALID_SECRET);
        var ctx = new StubRequestContext(path, null);

        filter.filter(ctx);

        assertThat(ctx.aborted)
                .as("Missing token must be rejected")
                .isTrue();
    }

    @Provide
    Arbitrary<String> requestPaths() {
        return Arbitraries.of("v1/workers/register", "v1/worker/heartbeat", "v1/query");
    }

    @Provide
    Arbitrary<String> internalPaths() {
        return Arbitraries.of("v1/workers/register", "v1/worker/heartbeat", "v1/task/status");
    }

    @Provide
    Arbitrary<String> userFacingPaths() {
        return Arbitraries.of("v1/query", "v1/catalog", "v1/statement");
    }

    @Provide
    Arbitrary<String> invalidTokens() {
        return Arbitraries.of(
                "Bearer wrong-secret",
                "Bearer ",
                "Basic dXNlcjpwYXNz",
                "Token " + VALID_SECRET);
    }

    /** AuthenticationManager that rejects all requests — used to verify internal token bypass. */
    static final class RejectAllAuthenticationManager extends AuthenticationManager {
        RejectAllAuthenticationManager() {
            super(List.of());
        }

        @Override
        public AuthenticationResult authenticate(AuthenticationRequest request) {
            return AuthenticationResult.failure("Rejected by test");
        }
    }

    /** Minimal ContainerRequestContext for filter testing without JAX-RS runtime. */
    static final class StubRequestContext implements ContainerRequestContext {
        private final String path;
        private final String authHeader;
        boolean aborted;

        StubRequestContext(String path, String authHeader) {
            this.path = path;
            this.authHeader = authHeader;
        }

        @Override public String getHeaderString(String name) {
            if ("Authorization".equalsIgnoreCase(name)) return authHeader;
            return null;
        }
        @Override public boolean containsHeaderString(
                String name, String value, java.util.function.Predicate<String> valuePredicate) {
            String header = getHeaderString(name);
            return header != null && valuePredicate.test(header);
        }

        @Override public UriInfo getUriInfo() {
            return new StubUriInfo(path);
        }

        @Override public void abortWith(jakarta.ws.rs.core.Response response) {
            aborted = true;
        }

        @Override public void setProperty(String name, Object object) {}
        @Override public Object getProperty(String name) { return null; }
        @Override public java.util.Collection<String> getPropertyNames() { return java.util.List.of(); }
        @Override public void removeProperty(String name) {}
        @Override public MultivaluedMap<String, String> getHeaders() { return new MultivaluedHashMap<>(); }
        @Override public java.util.Date getDate() { return null; }
        @Override public java.util.Locale getLanguage() { return null; }
        @Override public int getLength() { return 0; }
        @Override public jakarta.ws.rs.core.MediaType getMediaType() { return null; }
        @Override public java.util.List<jakarta.ws.rs.core.MediaType> getAcceptableMediaTypes() { return java.util.List.of(); }
        @Override public java.util.List<java.util.Locale> getAcceptableLanguages() { return java.util.List.of(); }
        @Override public java.util.Map<String, jakarta.ws.rs.core.Cookie> getCookies() { return java.util.Map.of(); }
        @Override public boolean hasEntity() { return false; }
        @Override public java.io.InputStream getEntityStream() { return java.io.InputStream.nullInputStream(); }
        @Override public void setEntityStream(java.io.InputStream input) {}
        @Override public jakarta.ws.rs.core.SecurityContext getSecurityContext() { return null; }
        @Override public void setSecurityContext(jakarta.ws.rs.core.SecurityContext context) {}
        @Override public String getMethod() { return "POST"; }
        @Override public void setMethod(String method) {}
        @Override public void setRequestUri(URI requestUri) {}
        @Override public void setRequestUri(URI baseUri, URI requestUri) {}
        @Override public jakarta.ws.rs.core.Request getRequest() { return null; }
    }

    static final class StubUriInfo implements UriInfo {
        private final String path;
        StubUriInfo(String path) { this.path = path; }
        @Override public String getPath() { return path; }
        @Override public String getPath(boolean decode) { return path; }
        @Override public java.util.List<jakarta.ws.rs.core.PathSegment> getPathSegments() { return java.util.List.of(); }
        @Override public java.util.List<jakarta.ws.rs.core.PathSegment> getPathSegments(boolean decode) { return java.util.List.of(); }
        @Override public URI getRequestUri() { return URI.create("http://localhost/" + path); }
        @Override public jakarta.ws.rs.core.UriBuilder getRequestUriBuilder() { return null; }
        @Override public URI getAbsolutePath() { return getRequestUri(); }
        @Override public jakarta.ws.rs.core.UriBuilder getAbsolutePathBuilder() { return null; }
        @Override public URI getBaseUri() { return URI.create("http://localhost/"); }
        @Override public jakarta.ws.rs.core.UriBuilder getBaseUriBuilder() { return null; }
        @Override public MultivaluedMap<String, String> getPathParameters() { return new MultivaluedHashMap<>(); }
        @Override public MultivaluedMap<String, String> getPathParameters(boolean decode) { return new MultivaluedHashMap<>(); }
        @Override public MultivaluedMap<String, String> getQueryParameters() { return new MultivaluedHashMap<>(); }
        @Override public MultivaluedMap<String, String> getQueryParameters(boolean decode) { return new MultivaluedHashMap<>(); }
        @Override public java.util.List<String> getMatchedURIs() { return java.util.List.of(); }
        @Override public String getMatchedResourceTemplate() { return path; }
        @Override public java.util.List<String> getMatchedURIs(boolean decode) { return java.util.List.of(); }
        @Override public java.util.List<Object> getMatchedResources() { return java.util.List.of(); }
        @Override public URI resolve(URI uri) { return uri; }
        @Override public URI relativize(URI uri) { return uri; }
    }
}
