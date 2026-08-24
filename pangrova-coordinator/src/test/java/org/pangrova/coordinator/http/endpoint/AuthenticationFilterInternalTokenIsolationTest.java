package org.pangrova.coordinator.http.endpoint;

import static org.assertj.core.api.Assertions.assertThat;

import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.core.Response;
import org.pangrova.common.http.EmbeddedHttpServerFactory;
import org.pangrova.security.authn.AuthenticationManager;
import org.pangrova.security.authn.AuthenticationRequest;
import org.pangrova.security.authn.AuthenticationResult;
import org.pangrova.security.authn.Authenticator;
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
import java.util.List;
import java.util.Map;

/**
 * Regression test for F-CHAIN-SEC-06: an internal service token must be confined to dedicated
 * internal RPC routes and must NEVER reach the user query endpoint without a resolved identity.
 *
 * <p>The {@link AuthenticationFilter} is configured with an internal secret and an
 * {@code AlwaysRejectAuthenticator} (so the only way to pass auth is the internal token). The
 * test asserts that the internal token is honored on an internal route ({@code /v1/worker}) but
 * is rejected with HTTP 403 on the user query endpoint ({@code /v1/query}).</p>
 */
class AuthenticationFilterInternalTokenIsolationTest {

    private static final String SECRET = "internal-secret-xyz";

    private static Server server;
    private static int port;
    private static final HttpClient HTTP_CLIENT = HttpClient.newHttpClient();

    @BeforeAll
    static void startServer() throws Exception {
        // Reject every external credential so passing auth is only possible via the internal token.
        AuthenticationManager authManager =
                new AuthenticationManager(List.of(new AlwaysRejectAuthenticator()));
        AuthenticationFilter authFilter = new AuthenticationFilter(authManager, SECRET);

        ResourceConfig config = new ResourceConfig();
        config.register(authFilter);
        config.register(QueryResourceStub.class);
        config.register(WorkerResourceStub.class);

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
    void internalTokenRejectedOnUserQueryEndpoint() throws Exception {
        HttpResponse<String> response = sendGet("/v1/query/submit", "Bearer " + SECRET);

        assertThat(response.statusCode())
                .as("internal service token must not reach the user query endpoint")
                .isEqualTo(403);
        assertThat(response.body()).contains("FORBIDDEN");
    }

    @Test
    void internalTokenAllowedOnInternalRoute() throws Exception {
        HttpResponse<String> response = sendGet("/v1/worker/heartbeat", "Bearer " + SECRET);

        assertThat(response.statusCode())
                .as("internal service token is honored on dedicated internal RPC routes")
                .isEqualTo(200);
        assertThat(response.body()).isEqualTo("worker-ok");
    }

    @Test
    void nonSecretTokenOnUserEndpointStillRejectedByAuth() throws Exception {
        // A non-internal token is not an internal token, so it falls through to normal auth,
        // which rejects it with 401 (not 403).
        HttpResponse<String> response = sendGet("/v1/query/submit", "Bearer not-the-secret");

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
    public static class QueryResourceStub {
        @GET
        @Path("/submit")
        public Response submit() {
            return Response.ok("query-ok").build();
        }
    }

    @Path("/v1/worker")
    public static class WorkerResourceStub {
        @GET
        @Path("/heartbeat")
        public Response heartbeat() {
            return Response.ok("worker-ok").build();
        }
    }

    /** Authenticator that rejects every credential, simulating strict OAuth2/LDAP. */
    private static final class AlwaysRejectAuthenticator implements Authenticator {
        @Override
        public String getName() { return "always-reject"; }

        @Override
        public void init(Map<String, String> config) { }

        @Override
        public AuthenticationResult authenticate(AuthenticationRequest request) {
            return AuthenticationResult.failure("rejected");
        }

        @Override
        public boolean supports(AuthenticationRequest request) { return true; }

        @Override
        public boolean isHealthy() { return true; }

        @Override
        public void close() { }
    }
}
