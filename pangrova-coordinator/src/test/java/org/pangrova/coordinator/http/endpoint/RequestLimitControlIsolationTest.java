package org.pangrova.coordinator.http.endpoint;

import jakarta.ws.rs.GET;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.core.Context;
import jakarta.ws.rs.container.ContainerRequestContext;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.servlet.ServletContainer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.pangrova.common.http.EmbeddedHttpServerFactory;
import org.pangrova.coordinator.http.RequestLimitConfig;
import org.pangrova.security.authn.AuthenticationManager;
import org.pangrova.security.authn.AuthenticationRequest;
import org.pangrova.security.authn.AuthenticationResult;
import org.pangrova.security.authn.Authenticator;
import org.pangrova.spi.security.UserIdentity;

import static org.assertj.core.api.Assertions.assertThat;

/** Real TCP authentication/filter isolation, not a Worker execution receipt. */
class RequestLimitControlIsolationTest {
    private static final String SECRET = "control-isolation-test-secret";
    private static final int LIMIT = 2;
    private final AtomicReference<String> peer = new AtomicReference<>();
    private final HttpClient client = HttpClient.newHttpClient();
    private Server server;
    private URI base;

    @BeforeEach
    void start() throws Exception {
        var authentication = new AuthenticationManager(List.of(new PeerAuthenticator(peer)));
        var config = new ResourceConfig().register(RequestPeerAddressFilter.class)
                .register(new RequestLimitFilter(new RequestLimitConfig(128, LIMIT)))
                .register(new AuthenticationFilter(authentication, SECRET))
                .register(CoordinatorExceptionMapper.class)
                .register(org.glassfish.jersey.jackson.JacksonFeature.class)
                .register(UserEndpoint.class).register(ControlEndpoint.class);
        var handler = new ServletContextHandler(ServletContextHandler.NO_SESSIONS);
        handler.setContextPath("/");
        handler.addServlet(new ServletHolder(new ServletContainer(config)), "/*");
        server = EmbeddedHttpServerFactory.create(0, handler);
        server.start();
        base = URI.create("http://127.0.0.1:" + EmbeddedHttpServerFactory.getPort(server));
    }

    @AfterEach
    void stop() throws Exception {
        if (server != null) server.stop();
    }

    @Test
    void authenticatorReceivesActualPeerNotForwardedHeader() throws Exception {
        assertThat(get("/v1/query", "Bearer user", "203.0.113.1").statusCode()).isEqualTo(200);
        assertThat(peer).hasValue("127.0.0.1");
    }

    @Test
    void callerCannotRotateForwardedHeaderToEvadeLimit() throws Exception {
        for (int i = 0; i < LIMIT; i++) {
            assertThat(get("/v1/query", "Bearer user", "203.0.113." + i).statusCode()).isEqualTo(200);
        }
        assertThat(get("/v1/query", "Bearer user", "203.0.113.99").statusCode()).isEqualTo(429);
    }

    @Test
    void authenticatedControlRemainsAvailableAfterPublicBudgetIsExhausted() throws Exception {
        for (int i = 0; i < LIMIT; i++) {
            assertThat(get("/v1/query", "Bearer user", null).statusCode()).isEqualTo(200);
        }
        assertThat(get("/v1/query", "Bearer user", null).statusCode()).isEqualTo(429);
        for (int i = 0; i < LIMIT + 1; i++) {
            assertThat(get("/v1/worker/heartbeat", "Bearer " + SECRET, null).statusCode()).isEqualTo(200);
        }
        assertThat(get("/v1/worker/heartbeat", "Bearer forged", null).statusCode()).isEqualTo(401);
        assertThat(get("/v1/query", "Bearer " + SECRET, null).statusCode()).isEqualTo(403);
    }

    @Test
    void authenticatedControlStillEnforcesBodyLimit() throws Exception {
        var request = HttpRequest.newBuilder(base.resolve("/v1/worker/heartbeat"))
                .header("Authorization", "Bearer " + SECRET)
                .POST(HttpRequest.BodyPublishers.ofByteArray(new byte[129])).build();
        assertThat(client.send(request, HttpResponse.BodyHandlers.ofString()).statusCode()).isEqualTo(413);
    }

    @Test
    void exactBodyLimitIsAllowed() throws Exception {
        var exact = HttpRequest.newBuilder(base.resolve("/v1/worker/heartbeat"))
                .header("Authorization", "Bearer " + SECRET)
                .POST(HttpRequest.BodyPublishers.ofByteArray(new byte[128])).build();
        var accepted = client.send(exact, HttpResponse.BodyHandlers.ofString());
        assertThat(accepted.statusCode()).isEqualTo(200);
        assertThat(accepted.body()).isEqualTo("128");
    }

    @Test
    void chunkedOversizeIsRejectedWithClientError() throws Exception {
        var chunked = HttpRequest.newBuilder(base.resolve("/v1/worker/heartbeat"))
                .header("Authorization", "Bearer " + SECRET)
                .POST(HttpRequest.BodyPublishers.ofInputStream(
                        () -> new java.io.ByteArrayInputStream(new byte[129]))).build();
        var rejected = client.send(chunked, HttpResponse.BodyHandlers.ofString());
        assertThat(rejected.statusCode()).isEqualTo(413);
        assertThat(rejected.body()).contains("PAYLOAD_TOO_LARGE");
    }

    private HttpResponse<String> get(String path, String auth, String forwarded) throws Exception {
        var builder = HttpRequest.newBuilder(base.resolve(path)).header("Authorization", auth).GET();
        if (forwarded != null) builder.header("X-Forwarded-For", forwarded);
        return client.send(builder.build(), HttpResponse.BodyHandlers.ofString());
    }

    @Path("/v1/query")
    public static class UserEndpoint {
        @GET
        public String get() { return "user"; }
    }

    @Path("/v1/worker/heartbeat")
    public static class ControlEndpoint {
        @GET
        public String get(@Context ContainerRequestContext context) {
            AuthenticationFilter.requireInternalServiceIdentity(context);
            return "control";
        }

        @POST
        public String post(@Context ContainerRequestContext context, byte[] body) {
            AuthenticationFilter.requireInternalServiceIdentity(context);
            return Integer.toString(body.length);
        }
    }

    private record PeerAuthenticator(AtomicReference<String> peer) implements Authenticator {
        @Override public String getName() { return "peer-test"; }
        @Override public void init(Map<String, String> config) { }
        @Override public boolean supports(AuthenticationRequest request) { return true; }
        @Override public boolean isHealthy() { return true; }
        @Override public void close() { }
        @Override public AuthenticationResult authenticate(AuthenticationRequest request) {
            peer.set(request.clientIp());
            if (!"user".equals(request.token())) return AuthenticationResult.failure("invalid test token");
            return AuthenticationResult.success(new UserIdentity("user", Set.of(), Map.of(), Instant.now()));
        }
    }
}
