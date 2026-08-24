package org.pangrova.coordinator.http;

import static org.assertj.core.api.Assertions.assertThat;

import org.pangrova.coordinator.query.task.TaskStatusReceiver;
import org.pangrova.coordinator.query.task.TaskStatusReceiverContext;
import org.pangrova.coordinator.query.QueryManager;
import org.pangrova.coordinator.query.QueryManagerContext;
import org.pangrova.coordinator.task.CoordinatorTaskTracker;
import org.pangrova.coordinator.worker.WorkerManager;
import org.pangrova.observability.noop.NoOpTracingProvider;
import org.pangrova.security.authn.AuthenticationManager;
import org.pangrova.security.authn.AuthenticationRequest;
import org.pangrova.security.authn.AuthenticationResult;
import org.pangrova.security.authn.Authenticator;
import org.pangrova.spi.coordination.inmemory.InMemoryWorkerRegistry;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.List;
import java.util.Map;

/**
 * Regression test for Task 47 / F-COORD-HTTP-01: the internal-auth shared secret was never
 * wired into the coordinator {@code AuthenticationFilter}, so with user OAuth2/LDAP auth enabled,
 * worker→coordinator internal calls (heartbeat/registration/task status) carrying the internal
 * secret were wrongly rejected with HTTP 401.
 *
 * <p>This test starts a real {@link CoordinatorHttpServer} with an {@link AuthenticationManager}
 * that rejects every external credential (simulating strict OAuth2/LDAP) plus an internal secret
 * configured via {@link CoordinatorHttpServerContext#internalAuthSecret()}. It asserts that an
 * internal request carrying the secret is accepted (NOT 401), while external requests without the
 * secret are still rejected.</p>
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class InternalAuthSecretWiringTest {

    private static final String SECRET = "super-internal-secret";

    private CoordinatorHttpServer httpServer;
    private HttpClient httpClient;
    private String baseUrl;

    @BeforeAll
    void setUp() throws Exception {
        // AuthenticationManager that rejects every external credential (strict OAuth2/LDAP).
        AuthenticationManager authManager =
                new AuthenticationManager(List.of(new AlwaysRejectAuthenticator()));

        var tracing = new NoOpTracingProvider();
        var queryManager = new QueryManager(QueryManagerContext.minimal(null, tracing));
        var taskTracker = new CoordinatorTaskTracker();
        var workerManager = new WorkerManager(20_000, 60_000, new InMemoryWorkerRegistry());
        var receiverContext = TaskStatusReceiverContext.withoutSpeculation(
                queryManager, taskTracker, workerManager);
        var receiver = new TaskStatusReceiver(receiverContext);

        var serverContext = new CoordinatorHttpServerContext(
                receiver, queryManager, null, authManager, null, workerManager,
                null, null, null, null, null, SECRET);

        httpServer = new CoordinatorHttpServer(0, serverContext);
        httpServer.start();
        baseUrl = "http://localhost:" + httpServer.getPort();
        httpClient = HttpClient.newHttpClient();
    }

    @AfterAll
    void tearDown() throws Exception {
        if (httpServer != null) {
            httpServer.stop();
        }
    }

    @Test
    void internalRequestWithSecretIsNotRejectedWhenOAuthEnabled() throws Exception {
        // Worker heartbeat is a real internal endpoint registered by WorkerResource.
        HttpResponse<String> response = post("/v1/worker/heartbeat", "{}", "Bearer " + SECRET);

        // The internal secret must short-circuit the OAuth2/LDAP filter. Before the fix the
        // filter never received the secret, so this returned 401.
        assertThat(response.statusCode())
                .as("internal request carrying the secret must bypass OAuth2/LDAP auth")
                .isNotEqualTo(401);
    }

    @Test
    void externalRequestWithoutSecretIsStillRejected() throws Exception {
        HttpResponse<String> response = post("/v1/worker/heartbeat", "{}", "Bearer not-the-secret");

        assertThat(response.statusCode())
                .as("external requests without the internal secret must still be 401 under OAuth2/LDAP")
                .isEqualTo(401);
    }

    @Test
    void externalRequestWithNoAuthHeaderIsStillRejected() throws Exception {
        HttpResponse<String> response = post("/v1/worker/heartbeat", "{}", null);

        assertThat(response.statusCode()).isEqualTo(401);
    }

    private HttpResponse<String> post(String path, String body, String authHeader)
            throws Exception {
        HttpRequest.Builder builder = HttpRequest.newBuilder()
                .uri(URI.create(baseUrl + path))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(body));
        if (authHeader != null) {
            builder.header("Authorization", authHeader);
        }
        return httpClient.send(builder.build(), HttpResponse.BodyHandlers.ofString());
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
