package org.pangrova.coordinator.http.endpoint;

import static org.assertj.core.api.Assertions.assertThat;

import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.core.Response;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.time.Instant;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.pangrova.common.http.EmbeddedHttpServerFactory;
import org.pangrova.security.authn.AuthenticationManager;
import org.pangrova.security.authn.AuthenticationRequest;
import org.pangrova.security.authn.AuthenticationResult;
import org.pangrova.security.authn.Authenticator;
import org.pangrova.security.authn.BruteForceGuard;
import org.pangrova.spi.security.UserIdentity;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.servlet.ServletContainer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Integration test for {@link AuthenticationFilter}'s {@link BruteForceGuard} wiring
 * (Task 7.6, Requirement 8.6).
 *
 * <p>Verifies that after the configured threshold of failed authentication attempts from
 * the same client IP, the filter short-circuits with HTTP 429 (Too Many Requests) before
 * even invoking the AuthenticationManager. Also verifies per-IP independence and that a
 * successful authentication immediately clears the failure counter.
 *
 * <p>Each test uses a unique X-Forwarded-For value so per-IP buckets in the shared guard
 * do not interfere across tests.
 *
 * <p>No mocks: a real Jetty + Jersey server with a real {@link BruteForceGuard}.
 * Each test gets a fresh server to avoid cross-test state leakage between the
 * BruteForceGuard's per-IP counters and the AuthenticationManager's health tracking.
 */
class AuthenticationFilterBruteForceIntegrationTest {

    private static final int THRESHOLD = 3;
    private static final HttpClient HTTP_CLIENT = HttpClient.newHttpClient();
    private Server server;
    private int port;

    @BeforeEach
    void startServer() throws Exception {
        AuthenticationManager authManager = new AuthenticationManager(
                List.of(new TestBasicAuthenticator()));
        // Use a threshold below AuthenticationManager.UNHEALTHY_THRESHOLD (5)
        // to prevent bad-credential rejections from marking the authenticator unhealthy.
        BruteForceGuard guard = new BruteForceGuard(THRESHOLD, Duration.ofMinutes(5));
        AuthenticationFilter filter = new AuthenticationFilter(authManager, null, guard);

        ResourceConfig config = new ResourceConfig();
        config.register(filter);
        config.register(ProtectedResource.class);
        config.register(HealthResource.class);

        ServletContextHandler handler = new ServletContextHandler(ServletContextHandler.NO_SESSIONS);
        handler.setContextPath("/");
        handler.addServlet(new ServletHolder(new ServletContainer(config)), "/*");

        server = EmbeddedHttpServerFactory.create(0, handler);
        server.start();
        port = EmbeddedHttpServerFactory.getPort(server);
    }

    @AfterEach
    void stopServer() throws Exception {
        if (server != null) {
            server.stop();
        }
    }

    @Test
    void fifthFailureFromSameIpTriggersLockout() throws Exception {
        String ip = "10.50.0.1";

        // Attempts 1..5: bad credentials are rejected with 401 by AuthenticationFilter,
        // each one increments the per-IP failure counter.
        for (int i = 1; i <= THRESHOLD; i++) {
            HttpResponse<String> resp = sendBadAuth(ip);
            assertThat(resp.statusCode())
                    .as("attempt %d should be 401 (auth failure)", i)
                    .isEqualTo(401);
        }

        // 6th attempt from same IP: BruteForceGuard short-circuits with 429
        // BEFORE the AuthenticationManager is invoked.
        HttpResponse<String> blocked = sendBadAuth(ip);
        assertThat(blocked.statusCode()).isEqualTo(429);
        assertThat(blocked.body()).contains("TOO_MANY_REQUESTS");
    }

    @Test
    void lockoutDeniesEvenValidCredentials() throws Exception {
        String ip = "10.50.0.2";

        // Exhaust the threshold with bad credentials.
        for (int i = 0; i < THRESHOLD; i++) {
            assertThat(sendBadAuth(ip).statusCode()).isEqualTo(401);
        }

        // Even a request with VALID credentials must be blocked while locked out:
        // the guard runs before authentication.
        HttpResponse<String> blockedValid = sendGoodAuth(ip);
        assertThat(blockedValid.statusCode()).isEqualTo(429);
    }

    @Test
    void differentIpsAreIndependent() throws Exception {
        String attackerIp = "10.50.0.3";
        String legitIp = "10.50.0.4";

        // Attacker exhausts threshold and gets locked out.
        for (int i = 0; i < THRESHOLD; i++) {
            assertThat(sendBadAuth(attackerIp).statusCode()).isEqualTo(401);
        }
        assertThat(sendBadAuth(attackerIp).statusCode()).isEqualTo(429);

        // Different IP is unaffected: bad credentials still produce 401, not 429.
        assertThat(sendBadAuth(legitIp).statusCode()).isEqualTo(401);
        // Different IP can still authenticate successfully.
        assertThat(sendGoodAuth(legitIp).statusCode()).isEqualTo(200);
    }

    @Test
    void successResetsFailureCounter() throws Exception {
        String ip = "10.50.0.5";

        // Fail THRESHOLD - 1 times (just under the lockout threshold).
        for (int i = 0; i < THRESHOLD - 1; i++) {
            assertThat(sendBadAuth(ip).statusCode()).isEqualTo(401);
        }

        // A successful auth must reset the per-IP counter.
        assertThat(sendGoodAuth(ip).statusCode()).isEqualTo(200);

        // After reset, the IP should tolerate another THRESHOLD - 1 failures
        // without being blocked (i.e., counter started over from zero).
        for (int i = 0; i < THRESHOLD - 1; i++) {
            assertThat(sendBadAuth(ip).statusCode())
                    .as("post-reset failure %d should be 401, not 429", i + 1)
                    .isEqualTo(401);
        }
    }

    @Test
    void livenessRemainsAvailableAfterAuthenticationLockout() throws Exception {
        String ip = "10.50.0.6";
        for (int i = 0; i < THRESHOLD; i++) {
            assertThat(sendBadAuth(ip).statusCode()).isEqualTo(401);
        }
        assertThat(sendBadAuth(ip).statusCode()).isEqualTo(429);

        for (int i = 0; i < THRESHOLD + 1; i++) {
            assertThat(sendGet("/v1/health/live", null, ip).statusCode()).isEqualTo(200);
        }
        assertThat(sendGet("/v1/health/live/extra", null, ip).statusCode()).isEqualTo(429);
        assertThat(sendBadAuth(ip).statusCode()).isEqualTo(429);
    }

    // --- HTTP helpers ---

    private HttpResponse<String> sendBadAuth(String forwardedFor)
            throws IOException, InterruptedException {
        String encoded = Base64.getEncoder().encodeToString("admin:wrong".getBytes());
        return sendGet("/v1/query/submit", "Basic " + encoded, forwardedFor);
    }

    private HttpResponse<String> sendGoodAuth(String forwardedFor)
            throws IOException, InterruptedException {
        String encoded = Base64.getEncoder().encodeToString("admin:secret".getBytes());
        return sendGet("/v1/query/submit", "Basic " + encoded, forwardedFor);
    }

    private HttpResponse<String> sendGet(String path, String authHeader, String forwardedFor)
            throws IOException, InterruptedException {
        HttpRequest.Builder builder = HttpRequest.newBuilder()
                .uri(URI.create("http://localhost:" + port + path))
                .GET();
        if (authHeader != null) {
            builder.header("Authorization", authHeader);
        }
        if (forwardedFor != null) {
            builder.header("X-Forwarded-For", forwardedFor);
        }
        return HTTP_CLIENT.send(builder.build(), HttpResponse.BodyHandlers.ofString());
    }

    // --- Test JAX-RS resource ---

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
        @Path("live")
        public Response live() {
            return Response.ok("live").build();
        }

        @GET
        @Path("live/extra")
        public Response nonHealthSubpath() {
            return Response.ok("protected").build();
        }
    }

    // --- Test authenticator (no mocks): accepts admin/secret, rejects everything else ---

    private static class TestBasicAuthenticator implements Authenticator {
        @Override public String getName() { return "test-basic"; }

        @Override public void init(Map<String, String> config) { }

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

        @Override public boolean isHealthy() { return true; }

        @Override public void close() { }
    }
}
