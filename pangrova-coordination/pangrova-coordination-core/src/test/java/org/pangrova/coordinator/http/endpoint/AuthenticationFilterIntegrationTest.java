package org.pangrova.coordinator.http.endpoint;

import static org.assertj.core.api.Assertions.assertThat;

import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.core.Response;
import org.pangrova.common.http.adapter.jetty.EmbeddedHttpServerFactory;
import org.pangrova.security.core.authn.AuthenticationManager;
import org.pangrova.spi.security.authn.AuthenticationRequest;
import org.pangrova.spi.security.authn.AuthenticationResult;
import org.pangrova.spi.security.authn.Authenticator;
import org.pangrova.spi.security.UserIdentity;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.servlet.ServletContainer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Instant;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Integration test for {@link AuthenticationFilter} using embedded Jetty + Jersey.
 * Sends real HTTP requests to verify authentication behavior end-to-end.
 */
class AuthenticationFilterIntegrationTest {

    private static Server server;
    private static int port;
    private static final HttpClient HTTP_CLIENT = HttpClient.newHttpClient();

    @BeforeAll
    static void startServer() throws Exception {
        AuthenticationManager authManager = new AuthenticationManager(
                List.of(new TestBearerAuthenticator(), new TestBasicAuthenticator()));
        AuthenticationFilter authFilter = new AuthenticationFilter(authManager);

        ResourceConfig config = new ResourceConfig();
        config.register(authFilter);
        config.register(ProtectedResource.class);
        config.register(HealthResource.class);

        ServletContextHandler handler = new ServletContextHandler(ServletContextHandler.NO_SESSIONS);
        handler.setContextPath("/");
        handler.addServlet(new ServletHolder(new ServletContainer(config)), "/*");

        server = EmbeddedHttpServerFactory.create(0, handler);
        server.start();
        port = EmbeddedHttpServerFactory.getPort(server);
    }

    @AfterAll
    static void stopServer() throws Exception {
        if (server != null) {
            server.stop();
        }
    }

    @Test
    void validBearerTokenReturns200() throws Exception {
        HttpResponse<String> response = sendGet("/v1/query/submit",
                "Bearer valid-token");

        assertThat(response.statusCode()).isEqualTo(200);
        assertThat(response.body()).isEqualTo("ok");
    }

    @Test
    void invalidBearerTokenReturns401() throws Exception {
        HttpResponse<String> response = sendGet("/v1/query/submit",
                "Bearer bad-token");

        assertThat(response.statusCode()).isEqualTo(401);
        assertThat(response.body()).contains("UNAUTHORIZED");
    }

    @Test
    void missingAuthHeaderReturns401() throws Exception {
        HttpResponse<String> response = sendGet("/v1/query/submit", null);

        assertThat(response.statusCode()).isEqualTo(401);
        assertThat(response.body()).contains("UNAUTHORIZED");
    }

    @Test
    void healthEndpointSkipsAuthentication() throws Exception {
        HttpResponse<String> response = sendGet("/v1/health", null);

        assertThat(response.statusCode()).isEqualTo(200);
        assertThat(response.body()).isEqualTo("healthy");
    }

    @Test
    void basicAuthValidCredentialsReturns200() throws Exception {
        String encoded = Base64.getEncoder()
                .encodeToString("admin:secret".getBytes());
        HttpResponse<String> response = sendGet("/v1/query/submit",
                "Basic " + encoded);

        assertThat(response.statusCode()).isEqualTo(200);
        assertThat(response.body()).isEqualTo("ok");
    }

    @Test
    void basicAuthInvalidCredentialsReturns401() throws Exception {
        String encoded = Base64.getEncoder()
                .encodeToString("admin:wrong".getBytes());
        HttpResponse<String> response = sendGet("/v1/query/submit",
                "Basic " + encoded);

        assertThat(response.statusCode()).isEqualTo(401);
    }

    private static HttpResponse<String> sendGet(String path, String authHeader)
            throws IOException, InterruptedException {
        HttpRequest.Builder builder = HttpRequest.newBuilder()
                .uri(URI.create("http://localhost:" + port + path))
                .GET();
        if (authHeader != null) {
            builder.header("Authorization", authHeader);
        }
        return HTTP_CLIENT.send(builder.build(), HttpResponse.BodyHandlers.ofString());
    }

    // --- Test JAX-RS resources ---

    @Path("/v1/query")
    public static class ProtectedResource {
        @GET
        @Path("/submit")
        public Response submit() {
            return Response.ok("ok").build();
        }
    }

    @Path("/v1/health")
    public static class HealthResource {
        @GET
        public Response health() {
            return Response.ok("healthy").build();
        }
    }

    // --- Test authenticators ---

    private static class TestBearerAuthenticator implements Authenticator {
        @Override
        public String getName() { return "test-bearer"; }

        @Override
        public void init(Map<String, String> config) { }

        @Override
        public AuthenticationResult authenticate(AuthenticationRequest request) {
            if ("valid-token".equals(request.token())) {
                return AuthenticationResult.success(
                        new UserIdentity("testuser", Set.of(), Map.of(), Instant.now()));
            }
            return AuthenticationResult.failure("Invalid token");
        }

        @Override
        public boolean supports(AuthenticationRequest request) {
            return request.token() != null;
        }

        @Override
        public boolean isHealthy() { return true; }

        @Override
        public void close() { }
    }

    private static class TestBasicAuthenticator implements Authenticator {
        @Override
        public String getName() { return "test-basic"; }

        @Override
        public void init(Map<String, String> config) { }

        @Override
        public AuthenticationResult authenticate(AuthenticationRequest request) {
            if ("admin".equals(request.username())
                    && request.password() != null
                    && java.util.Arrays.equals(request.password(), "secret".toCharArray())) {
                return AuthenticationResult.success(
                        new UserIdentity("admin", Set.of(), Map.of(), Instant.now()));
            }
            return AuthenticationResult.failure("Bad credentials");
        }

        @Override
        public boolean supports(AuthenticationRequest request) {
            return request.username() != null;
        }

        @Override
        public boolean isHealthy() { return true; }

        @Override
        public void close() { }
    }
}
